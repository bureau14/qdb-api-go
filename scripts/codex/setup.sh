#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status,
# if an undefined variable is used, or if any command in a pipeline fails.
set -euo pipefail

##
# Download dependencies for codex in the correct directory. Assumes to be invoked from the project root,
# assumes the latest nightly quasardb build.
#

QDB_PATH="$(pwd)/qdb/"

##
# Utility functions

function die {
    echo "$1"
    exit 1
}


##
# Download nightly quasardb artifacts and extract in qdb/ subdirectory.

if [ ! -d "${QDB_PATH}" ]
then
    echo "${QDB_PATH} does not yet exist, downloading and extracting tarballs into there."

    BASE="https://download.quasar.ai/quasardb/nightly/latest"
    VERSION="3.15.0.dev0"
    FILES=("qdb-${VERSION}-linux-64bit-c-api.tar.gz"
           "qdb-${VERSION}-linux-64bit-server.tar.gz"
           "qdb-${VERSION}-linux-64bit-utils.tar.gz")
    URLS=( "${BASE}/api/c/${FILES[0]}"
           "${BASE}/server/${FILES[1]}"
           "${BASE}/utils/${FILES[2]}")

    echo "Validating urls.."

    for URL in "${URLS[@]}"
    do
        echo "Checking: ${URL}..."
        curl -s -L --head --fail "${URL}" > /dev/null || die "Url not found: ${URL}"
    done

    echo "Extracting each tarball into qdb/ subdirectory"
    mkdir "${QDB_PATH}"
    pushd "${QDB_PATH}" > /dev/null

    for URL in "${URLS[@]}"
    do
        echo "Downloading and extracting: ${URL}..."
        curl -s -L "${URL}" | tar -xzf -
    done

    popd > /dev/null

    echo "Done downloading files quasardb dependencies: "
    find qdb/
else
    echo "${QDB_PATH} already exists, skip downloading dependencies"
fi


##
# Codex doesn't have network connectivity after the initial container is built, and
# as such all our go dependencies should be downloaded as part of the setup process.
#
# The easiest way to achieve this is just to run our teamcity's build script.
echo "Running build to download Go artifacts"

bash scripts/teamcity/10.build.sh
