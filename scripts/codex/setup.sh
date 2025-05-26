#!/usr/bin/env bash

echo "Initializing codex env: "

pwd

# Delete dir if exists
rm -rf qdb || true
mkdir qdb/

pushd qdb

curl -s -L https://download.quasar.ai/quasardb/3.14/3.14.1/api/c/qdb-3.14.1-linux-64bit-c-api.tar.gz | tar -xzf -
curl -s -L https://download.quasar.ai/quasardb/3.14/3.14.1/server/qdb-3.14.1-linux-64bit-server.tar.gz | tar -xzf -
curl -s -L https://download.quasar.ai/quasardb/3.14/3.14.1/utils/qdb-3.14.1-linux-64bit-utils.tar.gz | tar -xzf -

popd
