#!/usr/bin/env python3
"""Buildkite dynamic pipeline generator for qdb-api-go.

Step templates in steps/*.yml define nearly-complete Buildkite steps with
{placeholder} variables.  This script loads them, substitutes variables, and
overlays environment variables and the Docker plugin per platform.

Usage:
    python3 pipeline.py           # emit the snapshot gate (snapshot-bootstrap.yml)
    SNAPDEP_PHASE=generate python3 pipeline.py   # emit pipeline YAML to stdout
    python3 pipeline.py check     # validate without emitting
"""

from __future__ import annotations

# --- Snapshot dependencies shim (sc-20192) ----------------------------------
# Standard library only: runs before the tools submodule and third-party
# packages are imported.  A plain upload (no SNAPDEP_PHASE) prints the static
# gate jobs of snapshot-bootstrap.yml; the snapshot plugin then runs this
# script again with SNAPDEP_PHASE=generate to obtain the real pipeline.
# Any argument other than "generate" (e.g. "check") bypasses the shim.
import os
import shutil
import subprocess
import sys
import time

SNAPDEP_BOOTSTRAP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "snapshot-bootstrap.yml"
)


def _snapdep_wants_bootstrap() -> bool:
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"
    return (
        command == "generate"
        and os.environ.get("SNAPDEP_PHASE") != "generate"
        and os.path.exists(SNAPDEP_BOOTSTRAP)
    )


def _snapdep_record_arrival() -> None:
    """Set build meta-data snapdep-arrived; skipped outside a Buildkite job."""
    if not os.environ.get("BUILDKITE_JOB_ID") or shutil.which("buildkite-agent") is None:
        return
    arrived = str(int(time.time()))
    subprocess.run(["buildkite-agent", "meta-data", "set", "snapdep-arrived", arrived], check=True)


def _snapdep_head_commit() -> str:
    repo = os.path.dirname(SNAPDEP_BOOTSTRAP)
    out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True)
    return out.stdout.decode().strip()


def _snapdep_print_bootstrap() -> None:
    with open(SNAPDEP_BOOTSTRAP) as f:
        bootstrap = f.read()
    sys.stdout.write(bootstrap.replace("__COMMIT__", _snapdep_head_commit()))


if _snapdep_wants_bootstrap():
    _snapdep_record_arrival()
    _snapdep_print_bootstrap()
    sys.exit(0)
# --- end of snapshot dependencies shim --------------------------------------

import dataclasses
from pathlib import Path

from buildkite_sdk import CommandStep, Pipeline, GroupStep

sys.path.insert(0, str(Path(__file__).parent / "tools"))
from qdb_pipeline import (
    Platform,
    apply_docker,
    load_template,
    merge_env,
    select_platforms,
    validate_pipeline,
    get_git_ref,
    set_artifact_plugin_options,
)  # noqa: E402

STEPS_DIR = Path(__file__).parent / "steps"

# Quasardb-specific toolchain overlays on top of shared infrastructure platforms.
_LINUX = {
    "docker_image": "bureau14/builder:rhel7",
}
_WIN = dict()
_MACOS = dict()

_OS_OVERLAY = {"linux": _LINUX, "windows": _WIN, "macos": _MACOS}
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "linux-amd64-core2",
        "freebsd-amd64-haswell",
    )
]

BUILD_TYPES = ["Release"]

GO_VERSIONS = [
    "1.27",
]

# Environment variable layering: global → step → os → os+step → platform compilers.
GLOBAL_ENV: dict[str, str] = {
    "AWS_DEFAULT_REGION": "eu-west-1",
}

STEP_ENV: dict[str, dict[str, str]] = {}

# Linux builds run inside bureau14/builder:rhel7, whose default /usr/bin/gcc
# is 7.3.1; the static libqdb_api.a linked on Linux (library_link.go) is
# built with gcc16 and needs its libstdc++, so pin CC/CXX to the agent's
# gcc16.  The doubled $$ escapes Buildkite's upload-time interpolation so
# the literal $QDB_CICD_AGENT_GCC16_* reaches the agent shell, which
# substitutes the concrete path; the docker plugin propagates the resolved
# CC/CXX into the container.  Same idiom as _get_go_path_on_agent().
OS_ENV: dict[str, dict[str, str]] = {
    "linux": {
        "CC": "$$QDB_CICD_AGENT_GCC16_CC",
        "CXX": "$$QDB_CICD_AGENT_GCC16_CXX",
    },
    "freebsd": {},
    "macos": {},
    "windows": {},
}

OS_STEP_ENV: dict[str, dict[str, str]] = {}

CPU_ENV: dict[str, dict[str, str]] = {}


def _env(p: Platform, step_name: str, build_type: str) -> dict[str, str]:
    """Compose the full environment dict for one step."""
    return merge_env(
        GLOBAL_ENV,
        STEP_ENV.get(step_name, {}),
        OS_ENV.get(p.os, {}),
        OS_STEP_ENV.get(f"{p.os}/{step_name}", {}),
        CPU_ENV.get(p.cpu, {}),
        {"CMAKE_BUILD_TYPE": build_type},
        platform=p,
    )


def _get_go_path_on_agent(platform: Platform, go_version: str) -> dict[str, str]:
    """
    Returns environment variables to set for Go executable on the agent, based on platform and Go version.
    """
    go_slug = go_version.replace(".", "")
    go_env = {
        "GOPATH": f"$$QDB_CICD_AGENT_GO{go_slug}_PATH",
        "GOROOT": f"$$QDB_CICD_AGENT_GO{go_slug}_ROOT",
    }
    return go_env


def generate_pipeline() -> Pipeline:
    """Load templates, expand across platforms × build_types, overlay env and docker."""
    pipeline = Pipeline()
    git_ref = get_git_ref()
    group_steps = {}

    variants = []
    for p in PLATFORMS:
        for bt in BUILD_TYPES:
            for go in GO_VERSIONS:
                slug = p.slug(bt.lower(), f"go{go.replace('.', '')}")
                variants.append(slug)
                # We want to use Release QuasarDB binaries when building Go API (debug and release)
                dependency_slug = p.slug("release")

                tvars = {
                    "slug": slug,
                    "queue": (
                        f"{p.queue_os}-{p.arch}"
                        if p.os == "macos"
                        else f"default-{p.queue_os}-{p.arch}"
                    ),
                    "name": slug.replace("-", " ").title(),
                }

                artifact_vars_per_step = {
                    "download": {
                        "variant": dependency_slug,
                        "git_ref": git_ref,
                    },
                }

                step = load_template(STEPS_DIR / "_build.yml", **tvars)
                env = _env(p, "build", bt)
                env.update(step.get("env") or {})
                env.update(_get_go_path_on_agent(p, go))
                step["env"] = env
                apply_docker(step, p.docker_image)
                set_artifact_plugin_options(step, artifact_vars_per_step)

                # add step to group
                group_name = p.slug(bt.lower()).replace("-", " ").title()
                if group_name not in group_steps:
                    group_steps[group_name] = []
                group_steps[group_name].append(step)

    # create groups and add to pipeline
    for group, steps in group_steps.items():
        group_step = GroupStep(group=group, steps=steps)
        pipeline.add_step(group_step)
    
    # collect test reports
    step = load_template(STEPS_DIR / "_test_report.yml", **{"slug": "test-report"})
    step["depends_on"] = [f"build-{variant}" for variant in variants]
    pipeline.add_step(CommandStep.from_dict(step))

    return pipeline


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"

    try:
        pipeline = generate_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline generation failed: {e}", file=sys.stderr)
        sys.exit(1)

    if command == "generate":
        print(pipeline.to_yaml())
    elif command == "check":
        errors = validate_pipeline(pipeline)
        if errors:
            for e in errors:
                print(f"[FAIL] {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[OK] Pipeline valid: {len(pipeline.steps)} steps")
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Usage: pipeline.py [generate|check]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
