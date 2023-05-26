#!/bin/bash
set -e
DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$DIR/.."
FLOW_NAME="$1"
DEPLOYMENT_FILENAME="$FLOW_ENV-deployment-$FLOW_NAME.yaml"
ADDITIONAL_ARGS="${@:2}"

# Ensure environment variables are set
function test_env() {
  if [[ -z "$2" ]]; then 
    ERROR="$ERROR $1"
  fi
}
test_env "PREFECT_API_KEY" $PREFECT_API_KEY
test_env "PREFECT_API_URL" $PREFECT_API_URL
test_env "FLOW_ENV" $FLOW_ENV
test_env "FLOW_NAME" $FLOW_NAME

if [[ ! -z "${ERROR}" ]]; then
  echo "Some environnement variables must be set:$ERROR"
  exit 1 
fi


# Build and upload deployment file
prefect deployment build \
 --name "$FLOW_ENV"_"$FLOW_NAME" \
 --pool $FLOW_ENV-agent-pool \
 --work-queue default \
 --output $DEPLOYMENT_FILENAME \
 --param result_storage=s3-bucket/$FLOW_ENV \
 --storage-block=github/flows-$FLOW_ENV \
 --apply \
 $ADDITIONAL_ARGS \
 flows/$FLOW_NAME.py:"$FLOW_NAME"_flow

# Delete deployment file
rm $DEPLOYMENT_FILENAME
