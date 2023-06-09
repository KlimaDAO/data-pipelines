#!/bin/bash
set -e

function test_env() {
  if [[ -z "$2" ]]; then 
    ERROR="$ERROR $1"
  fi
}

test_env "PREFECT_POOL" $PREFECT_POOL
test_env "PREFECT_API_KEY" $PREFECT_API_KEY
test_env "PREFECT_API_URL" $PREFECT_API_URL
test_env "AWS_ACCESS_KEY_ID" $AWS_ACCESS_KEY_ID
test_env "AWS_SECRET_ACCESS_KEY" $AWS_SECRET_ACCESS_KEY

if [[ -z "${ERROR}" ]]; then
  prefect agent start -p $PREFECT_POOL
else
  echo "Some environnement variables must be set:$ERROR"
fi
