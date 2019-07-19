#!/bin/bash

set -xe

USER_LIST="users.cfg"
USER_PRIVATE_KEY="user_private.key"
CLUSTER_PUBLIC_KEY="cluster_public.key"
CLUSTER_PRIVATE_KEY="cluster_private.key"

DATA_DIR_INSECURE="insecure/db"
LOG_DIR_INSECURE="insecure/log"
URI_INSECURE="127.0.0.1:2836"

DATA_DIR_SECURE="secure/db"
LOG_DIR_SECURE="secure/log"
URI_SECURE="127.0.0.1:2837"
