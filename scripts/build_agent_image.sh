#!/bin/bash
set -e
DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$DIR/..
DOCKERFILE=$ROOT_DIR/agent/image/Dockerfile
docker build -f $DOCKERFILE -t data-pipelines-agent $ROOT_DIR
