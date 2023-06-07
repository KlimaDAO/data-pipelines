# data-pipelines

Data extraction and processing pipelines to power KlimaDAO products

## Repository structure

- agent: Files to build a docker image and make a Kubernetes deployment of a Prefect agent
- flows: Python flow files
- root directory: Deployment yaml files

## How does it work

- Deployments are installed on Prefect Cloud.
- Prefect Cloud Schedules flow-runs following the instructions in the configured deployments
- Agents pull flow-runs from their pool and executes them.
  - Agents download the flow code from github (Block github/flows)
  - Agents execute the python code. They use other blocks to store data (s3-bucket/dev for Development, s3-bucket/prod for production)

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

### Run flows

In the `flows directory`

You can run flows manually. For instance

`python raw_verra_data.py`

When running flows manually, the behaviour can be altered by creating a .env file. See `flows/.env.dist`

You can bootstrap your develpoment environment by running the special flow build_all. It will run all the flows in the right order

`python build_all.py`

## Manage deployments

Deployments are automatically uploaded to Prefect Cloud using github actions. To update deployments update the `.github/workflows/deploy-prefect-cloud.yaml` file

If you change the name of a deployment. The deployment with the previous name will still exist. You will need to delete it manually.

- View deployments
  prefect deployments ls

- Delete deployment
  prefect deployments delete <deployment_name>

## Launch an agent

To launch an agent on the workpool dev-agent-pool for instance

`prefect agent start -p dev-agent-pool`
