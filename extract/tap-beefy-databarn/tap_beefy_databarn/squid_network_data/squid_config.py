from tap_beefy_databarn.common.chains import ChainType, all_chains


# https://github.com/subsquid/archive-registry/blob/main/src/registry.ts
# https://cdn.subsquid.io/archives/evm.json
_squid_raw_archive_config = {
    "archives": [
        {"network": "arbitrum", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/arbitrum-one", "release": "ArrowSquid"}]},
        {"network": "arbitrum-goerli", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/arbitrum-goerli", "release": "ArrowSquid"}]},
        {"network": "arbitrum-nova", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/arbitrum-nova", "release": "ArrowSquid"}]},
        {"network": "arbitrum-sepolia", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/arbitrum-sepolia", "release": "ArrowSquid"}]},
        {"network": "astar", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/astar-mainnet", "release": "ArrowSquid"}]},
        {"network": "astar-zkatana", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/astar-zkatana", "release": "ArrowSquid"}]},
        {"network": "avalanche", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/avalanche-mainnet", "release": "ArrowSquid"}]},
        {"network": "avalanche-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/avalanche-testnet", "release": "ArrowSquid"}]},
        {"network": "base-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/base-mainnet", "release": "ArrowSquid"}]},
        {"network": "base-goerli", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/base-goerli", "release": "ArrowSquid"}]},
        {"network": "base-sepolia", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/base-sepolia", "release": "ArrowSquid"}]},
        {"network": "binance", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/binance-mainnet", "release": "ArrowSquid"}]},
        {"network": "binance-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/binance-testnet", "release": "ArrowSquid"}]},
        {"network": "dfk-chain", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/dfk-chain", "release": "ArrowSquid"}]},
        {"network": "eth-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/ethereum-mainnet", "release": "ArrowSquid"}]},
        {"network": "eth-goerli", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/ethereum-goerli", "release": "ArrowSquid"}]},
        {"network": "eth-holesky", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/ethereum-holesky", "release": "ArrowSquid"}]},
        {"network": "eth-sepolia", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/ethereum-sepolia", "release": "ArrowSquid"}]},
        {"network": "etherlink-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/etherlink-testnet", "release": "ArrowSquid"}]},
        {"network": "exosama", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/exosama", "release": "ArrowSquid"}]},
        {"network": "fantom", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/fantom-mainnet", "release": "ArrowSquid"}]},
        {"network": "fantom-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/fantom-testnet", "release": "ArrowSquid"}]},
        {"network": "flare-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/flare-mainnet", "release": "ArrowSquid"}]},
        {"network": "gnosis-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/gnosis-mainnet", "release": "ArrowSquid"}]},
        {"network": "linea-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/linea-mainnet", "release": "ArrowSquid"}]},
        {"network": "mineplex-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/mineplex-testnet", "release": "ArrowSquid"}]},
        {"network": "moonbase", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/moonbase-testnet", "release": "ArrowSquid"}]},
        {"network": "moonbeam", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/moonbeam-mainnet", "release": "ArrowSquid"}]},
        {"network": "moonriver", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/moonriver-mainnet", "release": "ArrowSquid"}]},
        {"network": "opbnb-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/opbnb-mainnet", "release": "ArrowSquid"}]},
        {"network": "opbnb-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/opbnb-testnet", "release": "ArrowSquid"}]},
        {"network": "optimism-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/optimism-mainnet", "release": "ArrowSquid"}]},
        {"network": "optimism-goerli", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/optimism-goerli", "release": "ArrowSquid"}]},
        {"network": "optimism-sepolia", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/optimism-sepolia", "release": "ArrowSquid"}]},
        {"network": "polygon", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/polygon-mainnet", "release": "ArrowSquid"}]},
        {"network": "polygon-mumbai", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/polygon-testnet", "release": "ArrowSquid"}]},
        {"network": "polygon-zkevm", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/polygon-zkevm-mainnet", "release": "ArrowSquid"}]},
        {"network": "polygon-zkevm-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/polygon-zkevm-testnet", "release": "ArrowSquid"}]},
        {"network": "sepolia", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/ethereum-sepolia", "release": "ArrowSquid"}]},
        {"network": "shibuya-testnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/shibuya-testnet", "release": "ArrowSquid"}]},
        {"network": "shiden-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/shiden-mainnet", "release": "ArrowSquid"}]},
        {"network": "skale-nebula", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/skale-nebula", "release": "ArrowSquid"}]},
        {"network": "tanssi", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/tanssi", "release": "ArrowSquid"}]},
        {"network": "zksync-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/zksync-mainnet", "release": "ArrowSquid"}]},
        {"network": "zora-mainnet", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/zora-mainnet", "release": "ArrowSquid"}]},
        {"network": "zora-goerli", "providers": [{"provider": "subsquid", "dataSourceUrl": "https://v2.archive.subsquid.io/network/zora-goerli", "release": "ArrowSquid"}]},
    ],
}

# ChainType to squid network name
_chain_map = {
    "arbitrum": "arbitrum",
    "aurora": None,
    "avax": "avalanche",
    "base": "base-mainnet",
    "bsc": "binance",
    "canto": None,
    "celo": None,
    "cronos": None,
    "emerald": None,
    "ethereum": "eth-mainnet",
    "fantom": "fantom",
    "fuse": None,
    "gnosis": "gnosis-mainnet",
    "one": None,
    "heco": None,
    "kava": None,
    "linea": "linea-mainnet",
    "metis": None,
    "moonbeam": "moonbeam",
    "moonriver": "moonriver",
    "optimism": "optimism-mainnet",
    "polygon": "polygon",
    "rollux": None,
    "scroll": None,
    "zkevm": "polygon-zkevm",
    "zksync": "zksync-mainnet",
}


def get_squid_archive_url(chain: ChainType) -> str | None:
    """Get the squid archive url for a given chain."""
    network_name = _chain_map[chain]
    if network_name is None:
        return None

    for archive in _squid_raw_archive_config["archives"]:
        if archive["network"] == network_name:
            return archive["providers"][0]["dataSourceUrl"]

    return None
