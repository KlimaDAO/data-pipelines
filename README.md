# data-pipelines

Data extraction and processing pipelines to power KlimaDAO products

## Setup a development environnement

This project requires python 3.7 or later

Create a python environnement and install dependencies

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
prefect cloud login
```

And follow instructions to setup your API key

## Manage deployments new deployment

### reate a deployment

see. https://docs.prefect.io/latest/concepts/deployments/#deployment-build-options for details

In the **deployemnts directory** run a command like:

`FLOW_ENV="dev" FLOW_NAME="raw_verra_data"; prefect deployment build \
 -n "$FLOW_ENV"_"$FLOW_NAME" \
 -p $FLOW_ENV-agent-pool \
 -q default \
 -o $FLOW_ENV-deployment-$FLOW_NAME.yaml \
 --param storage=s3/$FLOW_ENV \
 --storage-block=github/flows \
 --cron="0 0 * * *"  flows/$FLOW_NAME.py:$FLOW_NAME  `

- FLOW_ENV is test or prod
- FLOW_NAME is the name of the flow

- For production deployments

this will create a deployment file named **raw_verra_data-deployment.yaml** that you can customize

### Install a deployment

Edit the deployment file and run

`FLOW_ENV="test" FLOW_NAME="raw_verra_data"; prefect deployment apply $FLOW_ENV-deployment-$FLOW_NAME.yaml`

## Launch an agent

To launch an agent on the workpool default-agent-pool

`prefect agent start -p default-agent-pool`
