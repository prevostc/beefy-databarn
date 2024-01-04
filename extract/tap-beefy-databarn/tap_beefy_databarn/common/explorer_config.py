import typing as t
from dataclasses import dataclass

from tap_beefy_databarn.common.chains import ChainType


@dataclass
class ExplorerConfig:
    explorer_type: t.Literal["etherscan", "canto", "blockscout", "routescan", "blockscout-trx-list-api", "blockscout-v5", "harmony", "zksync"]
    url: str
    max_rps: float = 0.1 # 10s between requests

EXPLORER_CONFIG: dict[ChainType, ExplorerConfig] = {
  "arbitrum": ExplorerConfig(explorer_type="etherscan", url="https://api.arbiscan.io/api"),
  "aurora": ExplorerConfig(explorer_type="blockscout-v5", url="https://old.explorer.aurora.dev/api"),
  "avax": ExplorerConfig(explorer_type="routescan", url="https://api.routescan.io"),
  "base": ExplorerConfig(explorer_type="etherscan", url="https://api.basescan.org/api"),
  "bsc": ExplorerConfig(explorer_type="etherscan", url="https://api.bscscan.com/api"),
  "canto": ExplorerConfig(explorer_type="blockscout-v5", url="https://explorer.plexnode.wtf/api"),
  "celo": ExplorerConfig(explorer_type="blockscout", url="https://explorer.celo.org"),
  "cronos": ExplorerConfig(explorer_type="etherscan", url="https://api.cronoscan.com/api"),
  "emerald": ExplorerConfig(explorer_type="blockscout", url="https://explorer.emerald.oasis.dev/"),
  "ethereum": ExplorerConfig(explorer_type="etherscan", url="https://api.etherscan.io/api"),
  "fantom": ExplorerConfig(explorer_type="etherscan", url="https://api.ftmscan.com/api"),
  "fuse": ExplorerConfig(explorer_type="blockscout-v5", url="https://explorer.fuse.io/api"),
  "gnosis": ExplorerConfig(explorer_type="etherscan", url="https://api.gnosisscan.io/api"),
  "one": ExplorerConfig(explorer_type="harmony", url="https://explorer.harmony.one/"),
  "heco": ExplorerConfig(explorer_type="etherscan", url="https://api.hecoinfo.com/api"),
  "kava": ExplorerConfig(explorer_type="blockscout", url="https://kavascan.com"),
  "linea": ExplorerConfig(explorer_type="blockscout-trx-list-api", url="https://explorer.linea.build"),
  "metis": ExplorerConfig(explorer_type="blockscout", url="https://andromeda-explorer.metis.io"),
  "moonbeam": ExplorerConfig(explorer_type="etherscan", url="https://api-moonbeam.moonscan.io/api"),
  "moonriver": ExplorerConfig(explorer_type="etherscan", url="https://api-moonriver.moonscan.io/api"),
  "optimism": ExplorerConfig(explorer_type="etherscan", url="https://api-optimistic.etherscan.io/api"),
  "polygon": ExplorerConfig(explorer_type="etherscan", url="https://api.polygonscan.com/api"),
  "rollux": ExplorerConfig(explorer_type="blockscout-trx-list-api", url="https://explorer.rollux.com"),
  "scroll": ExplorerConfig(explorer_type="etherscan", url="https://scrollscan.com"),
  "zkevm": ExplorerConfig(explorer_type="etherscan", url="https://api-zkevm.polygonscan.com/api"),
  "zksync": ExplorerConfig(explorer_type="zksync", url="https://block-explorer-api.mainnet.zksync.io"),
}
