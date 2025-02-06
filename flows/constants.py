import os

GRAPH_API_KEY = os.environ.get('GRAPH_API_KEY')
GRAPH_BASE_URL = f'https://gateway-arbitrum.network.thegraph.com/api/{GRAPH_API_KEY}/subgraphs/id/'
GRAPH_DEV_BASE_URL = 'https://api.studio.thegraph.com/query/78559/'
GRAPH_VERSION_SUFFIX = '/version/latest'

CARBON_SG_ID = 'ECLEwJKgujmiRCW1XbfbbUbpae2igeHa2KJ6BXNSWrZF'
CARBON_ETH_SG_ID = 'A3dewmSVoxFvvyaMQUJFQHDg3cF8ageeq3hXHnMgs3kp'
CARBON_CELO_SG_ID = 'H5UMmbFgxE73i8s445cRuSx5bg7AUUrsKdtBBCQfinYX'
CARBON_HOLDERS_SG_ID = 'BJFQSmoqffMD8e1pN9xdsuRDYQNgVs8z3DBiXa6AdkGY'
PAIRS_SG_ID = 'hwk9GJd5cf5EASZRxQZJ56KocpYt7tNDtho9HZwN6rL'

if os.environ.get('ENV') == 'production':
    CARBON_SUBGRAPH_URL = GRAPH_BASE_URL + CARBON_SG_ID
    CARBON_ETH_SUBGRAPH_URL = GRAPH_BASE_URL + CARBON_ETH_SG_ID
    CARBON_CELO_SUBGRAPH_URL = GRAPH_BASE_URL + CARBON_CELO_SG_ID
    CARBON_HOLDERS_SUBGRAPH_URL = GRAPH_BASE_URL + CARBON_HOLDERS_SG_ID
    PAIRS_SUBGRAPH_URL = GRAPH_BASE_URL + PAIRS_SG_ID

else:
    CARBON_SUBGRAPH_URL = (
        GRAPH_DEV_BASE_URL + "staging-polygon-digital-carbon" + GRAPH_VERSION_SUFFIX
    )
    CARBON_ETH_SUBGRAPH_URL = (
        GRAPH_DEV_BASE_URL + "staging-ethereum-bridged-carbon" + GRAPH_VERSION_SUFFIX
    )
    CARBON_CELO_SUBGRAPH_URL = (
        GRAPH_DEV_BASE_URL + "staging-celo-bridged-carbon" + GRAPH_VERSION_SUFFIX
    )
    CARBON_HOLDERS_SUBGRAPH_URL = (
        GRAPH_DEV_BASE_URL + "staging-klimadao-user-carbon" + GRAPH_VERSION_SUFFIX
    )
    PAIRS_SUBGRAPH_URL = (
        GRAPH_DEV_BASE_URL + "staging-klimadao-pairs" + GRAPH_VERSION_SUFFIX
    )

BCT_ADDRESS = "0x2f800db0fdb5223b3c3f354886d907a671414a7f"
NCT_ADDRESS = "0xd838290e877e0188a4a44700463419ed96c16107"
UBO_ADDRESS = "0x2b3ecb0991af0498ece9135bcd04013d7993110c"
NBO_ADDRESS = "0x6bca3b77c1909ce1a4ba1a20d1103bde8d222e48"
MCO2_ADDRESS = "0xfC98e825A2264D890F9a1e68ed50E1526abCcacD"
MCO2_ADDRESS_MATIC = "0xaa7dbd1598251f856c12f63557a4c4397c253cea"
KLIMA_UBO_ADDRESS = "0x5400a05b8b45eaf9105315b4f2e31f806ab706de"
KLIMA_NBO_ADDRESS = "0x251ca6a70cbd93ccd7039b6b708d4cb9683c266c"
KLIMA_MCO2_ADDRESS = "0x64a3b8ca5a7e406a78e660ae10c7563d9153a739"
KLIMA_BCT_ADDRESS = "0x9803c7ae526049210a1725f7487af26fe2c24614"
BCT_KLIMA_ADDRESS = "0x9803c7ae526049210a1725f7487af26fe2c24614"
NCT_USDC_ADDRESS = "0xdb995f975f1bfc3b2157495c47e4efb31196b2ca"
KLIMA_USDC_ADDRESS = "0x5786b267d35F9D011c4750e0B0bA584E1fDbeAD1"

MISPRICED_NCT_SWAP_ID = "0xdb995f975f1bfc3b2157495c47e4efb31196b2ca1679432400"

BCT_DECIMALS = 18
C3_DECIMALS = 18
FRAX_DECIMALS = 18
KLIMA_DECIMALS = 9
MCO2_DECIMALS = 18
MOSS_DECIMALS = 18
NBO_DECIMALS = 18
UBO_DECIMALS = 18
NCT_DECIMALS = 18
USDC_DECIMALS = 12

TOKENS = {
    "BCT": {
        "Pair Address": BCT_KLIMA_ADDRESS,
        "Token Address": BCT_ADDRESS,
        "address": BCT_ADDRESS,
        "Full Name": "Base Carbon Tonne",
        "Bridge": "Toucan",
        "Decimals": BCT_DECIMALS,
    },
    "NCT": {
        "Pair Address": NCT_USDC_ADDRESS,
        "Token Address": NCT_ADDRESS,
        "address": NCT_ADDRESS,
        "Full Name": "Nature Carbon Tonne",
        "Bridge": "Toucan",
        "Decimals": NCT_DECIMALS,
    },
    "MCO2": {
        "Pair Address": KLIMA_MCO2_ADDRESS,
        "Token Address": MCO2_ADDRESS_MATIC,
        "address": MCO2_ADDRESS_MATIC,
        "Full Name": "Moss Carbon Credit",
        "Bridge": "Moss",
        "Decimals": MCO2_DECIMALS,
    },
    "UBO": {
        "Pair Address": KLIMA_UBO_ADDRESS,
        "Token Address": UBO_ADDRESS,
        "address": UBO_ADDRESS,
        "Full Name": "Universal Basic Offset",
        "Bridge": "C3",
        "Decimals": UBO_DECIMALS,
    },
    "NBO": {
        "Pair Address": KLIMA_NBO_ADDRESS,
        "Token Address": NBO_ADDRESS,
        "address": NBO_ADDRESS,
        "Full Name": "Nature Based Offset",
        "Bridge": "C3",
        "Decimals": NBO_DECIMALS,
    },
}
