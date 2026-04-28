#!/usr/bin/env python3
"""Buildkite dynamic pipeline generator for qdb-api-go.

Step templates in steps/*.yml define nearly-complete Buildkite steps with
{placeholder} variables.  This script loads them, substitutes variables, and
overlays environment variables and the Docker plugin per platform.

Usage:
    python3 pipeline.py           # emit pipeline YAML to stdout
    python3 pipeline.py check     # validate without emitting
"""
from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

from buildkite_sdk import Pipeline, GroupStep

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
        "freebsd-amd64-core2",
        "linux-amd64-core2",
        "linux-aarch64",
        "windows-amd64-core2",
        "macos-aarch64",
    )
]

BUILD_TYPES = ["Release"]

GO_VERSIONS = [
    "1.23",
    "1.24",
]

# Environment variable layering: global → step → os → os+step → platform compilers.
GLOBAL_ENV: dict[str, str] = {
    "AWS_DEFAULT_REGION": "eu-west-1",
}

STEP_ENV: dict[str, dict[str, str]] = {}

OS_ENV: dict[str, dict[str, str]] = {
    "linux": {
    },
    "freebsd": {
    },
    "macos": {},
    "windows": {},
}

OS_STEP_ENV: dict[str, dict[str, str]] = {}

CPU_ENV: dict[str, dict[str, str]] = {
    "aarch64": {},
}


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


def _get_agent_go_env(platform: Platform, go_version: str) -> dict[str, str]:
    """
    Returns environment variables to set for Go executable on the agent, based on platform and Go version.
    Applies to Windows and macOS where we have multiple Go versions installed in different locations.
    """
    # XXX (igor)
    # we can rely on referencing env variables instead of hardcoded paths but we need to update agents first to support this
    go_slug = go_version.replace(".", "")
    go_root_env = f"$${{QDB_CICD_AGENT_GO{go_slug}_GOROOT}}"
    go_path_env = f"$${{QDB_CICD_AGENT_GO{go_slug}_GOPATH}}"

    return {
        "GOROOT": go_root_env,
        "GOPATH": go_path_env,
    }


def _apply_doc_command(step: dict, platform: Platform) -> None:
    """
    Adds a command to the step to generate documentation using pdoc to linux-amd64-core2 platform builds.
    """
    if platform.os == "linux" :
        # TODO (igor): inspect what this does in teacmity and implement it here, if needed
        pass
        # doc_commands = [
        #     'echo "+++ Generate documentation"',  
        #     "bash scripts/teamcity/30.doc.sh",
        # ]
        # existing_commands = step.get("commands", [])
        # existing_commands += doc_commands


def generate_pipeline() -> Pipeline:
    """Load templates, expand across platforms × build_types, overlay env and docker."""
    pipeline = Pipeline()
    git_ref = get_git_ref()
    group_steps = {}

    for p in PLATFORMS:
        for bt in BUILD_TYPES:
            for go in GO_VERSIONS:
                slug = p.slug(bt.lower(), f"go{go.replace('.', '')}")

                # We want to use Release QuasarDB binaries when building Go API (debug and release)
                dependency_slug =  p.slug("release")

                tvars = {
                    "slug": slug,
                    "queue": f"{p.queue_os}-{p.arch}",
                    "name": slug.replace("-", " ").title(),
                    "go_root": _get_agent_go_env(p, go)["GOROOT"],
                    "go_path": _get_agent_go_env(p, go)["GOPATH"],
                }

                artifact_vars_per_step = {
                    "download": {"variant": dependency_slug, "git-ref": "refs/heads/sc-18547/buildkite"},
                }

                step = load_template(STEPS_DIR / "_build.yml", **tvars)
                env = _env(p, "test", bt)
                env.update(step.get("env") or {})
                step["env"] = env
                apply_docker(step, p.docker_image)
                set_artifact_plugin_options(step, artifact_vars_per_step)
                _apply_doc_command(step, p)

                # add step to group
                group_name = p.slug(bt.lower()).replace("-", " ").title()
                if group_name not in group_steps:
                    group_steps[group_name] = []
                group_steps[group_name].append(step)

    # create groups and add to pipeline
    for group, steps in group_steps.items():
        group_step = GroupStep(group=group, steps=steps)
        pipeline.add_step(group_step)

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
