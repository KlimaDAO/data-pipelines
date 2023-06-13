CARBON_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/polygon-bridged-carbon"
)
CARBON_MOSS_ETH_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/originalpkbims/ethcarbonsubgraph"
)
CARBON_ETH_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/ethereum-bridged-carbon"
)
CARBON_MOSS_ETH_TEST_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/originalpkbims/ethcarbonsubgraphtest"
)
CARBON_CELO_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/celo-bridged-carbon"
)
CARBON_HOLDERS_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/klimadao-user-carbon"
)
PAIRS_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/klimadao/klimadao-pairs"
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
BCT_USDC_ADDRESS = "0x1e67124681b402064cd0abe8ed1b5c79d2e02f64"
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
        "Pair Address": BCT_USDC_ADDRESS,
        "Token Address": BCT_ADDRESS,
        "address": BCT_ADDRESS,
        "id": "polygon-pos",
        "Full Name": "Base Carbon Tonne",
        "Bridge": "Toucan",
        "Decimals": BCT_DECIMALS,
    },
    "NCT": {
        "Pair Address": NCT_USDC_ADDRESS,
        "Token Address": NCT_ADDRESS,
        "address": NCT_ADDRESS,
        "id": "polygon-pos",
        "Full Name": "Nature Carbon Tonne",
        "Bridge": "Toucan",
        "Decimals": NCT_DECIMALS,
    },
    "MCO2": {
        "Pair Address": KLIMA_MCO2_ADDRESS,
        "Token address": MCO2_ADDRESS,
        "address": MCO2_ADDRESS,
        "id": "ethereum",
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
        "address": UBO_ADDRESS,
        "Full Name": "Nature Based Offset",
        "Bridge": "C3",
        "Decimals": NBO_DECIMALS,
    },
}
