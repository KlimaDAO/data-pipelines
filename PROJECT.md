# Introduction

This repository will host the Klimadao data pipeline. It aims to build a single source of truth for data used in the KlimaDAO dash-apps project. See: https://github.com/KlimaDAO/dash-apps


# Implementation strategy

The project will be implemented in multiple phases:

- Phase 1: 
 - set up barebones pipelines that dump raw data into object storage (e.g. AWS S3) as CSV 
 - reimplement the dashboard to load the data from object storage
- Phase 2: offload more of the aggregation and transformation logic to the data pipelines
- Phase 3: 
 - Implement an API to access the data from object storage
 - Reimplement the dash-app project as a proper JS frontend querying this API

## Phase 1

## Step 1: Reimplement all the querying done in the dash-apps project as Prefect flows.

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
- Protocol data (see yield_offset/app.py)
 - Trades
 - Protocol metris
 - Reward yield



## Phase 2

TODO

## Phase 3

TODO
