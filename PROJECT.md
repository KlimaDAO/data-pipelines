# Introduction

This repository will host the KlimaDAO data pipeline. It aims to build a single source of truth for data used in the KlimaDAO dash-apps project. See: https://github.com/KlimaDAO/dash-apps

# Implementation strategy

The project will be implemented in multiple phases:

- Phase 1:
- set up barebones pipelines that dump raw data into object storage (e.g. AWS S3) as CSV
- reimplement the dashboard to load the data from object storage
- Phase 2: offload more of the aggregation and transformation logic to the data pipelines
- Phase 3:
- Implement an API to access the data from object storage
- Reimplement the dash-app project as a proper JS frontend querying this API

## Phase 1: Reimplement all the querying done in the dash-apps project as Prefect flows (Done)

There should be one flow per dataset. The list of datasets is the following

- Polygon Subgraph
  - Carbon bridged (see get_data in tco2_dashboard/app.py)
  - Carbon retired (see get_data in tco2_dashboard/app.py)
  - Carbon deposited into pools (see get_data_pool in tco2_dashboard/app.py)
  - Carbon Redeemed carbon from pools (see get_data_pool in tco2_dashboard/app.py)
  - Carbon retired from pools (see get_data_pool_retired in tco2_dashboard/app.py)
  - Carbon metrics (see get_polygon_carbon_metrics in tco2_dashboard/app.py)
  - Carbon retired with Klima (see get_klima_retirements in tco2_dashboard/app.py)
  - Carbon retired with Klima daily (see dailyKlimaRetirements in tco2_dashboard/app.py)
- Ethereum Moss Subgraph
  - Carbon bridged (see get_mco2_data in tco2_dashboard/app.py)
  - Moss Carbon retired (see get_mco2_data in tco2_dashboard/app.py)
- Ethereum Subgraph
  - Carbon bridged transactions (see get_mco2_data in tco2_dashboard/app.py)
  - Carbon retired (see get_mco2_data in tco2_dashboard/app.py)
  - Carbon metrics (see get_eth_carbon_metrics in tco2_dashboard/app.py)
- Verra Data (see get_verra_data in tco2_dashboard/app.py)
- Holders data (see get_holders_data in tco2_dashboard/app.py)
- Celo data (see get_celo_carbon_metrics in tco2_dashboard/app.py)
- Asset prices (see get_prices in tco2_dashboard/app.py)

Notes:

- The Verra data is top priority
- The yield_offset dash-app is dead and should not be migrated

## Phase 2: offload more of the aggregation and transformation logic to the data pipelines

### Step 1 transformations (Done)

   - Column names homogeneisation among datasets
   - Offsets datasets
   - Tokens datasets
   - Price datasets
   - Token holders dataset

### Step 2: Aggregations

The goal of this step is to offload processing from the API. This will be done iteratively, starting with the most time consuming aggregations.

  - On offset datasets:
   - Daily quantitties aggregations
   - Monthly quantitties aggregations
   - Projects Aggregations
   - Methodologies aggregations
  - Carbon supply aggregation
  - Retirements aggregations
    - By pool
    - By token
    - By chain
    - By beneficiary

## Phase 3

### Implement an API to access the data from object storage

The API can be implemented using the flask-Restful framework which is based on Flask. This fits nicely with the knowledge the team has with Dash which is also based on Flask.

The API will query the S3 Objects created by the data-pipelines and provide final high level and low cost transformation features such as:
- Pagination
- Date range filtering
- CSV transformation

The API will be composed of two parts:
 - A Service layer, responsible for querying the S3 objects, transform them and cache the results
 - A Routing layer, responsible for routing queries to the appropriate services

The API will be developed in the dash-apps repo: https://github.com/KlimaDAO/dash-apps
See this issue: https://github.com/KlimaDAO/dash-apps/issues/154

### Reimplement the dash-app project as a proper JS frontend querying the API

The JS frontend can be implemented using the technologies already in use for the KlimaDAO site:
- NextJS
- recharts
- Additional charting librairies may be required

The JS Frontend can be developed in the KlimaDAO monorepo https://github.com/KlimaDAO/klimadao
See this issue: https://github.com/KlimaDAO/klimadao/issues/1277
