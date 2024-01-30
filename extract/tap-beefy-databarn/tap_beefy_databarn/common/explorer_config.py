from dataclasses import dataclass
from enum import StrEnum

from tap_beefy_databarn.common.chains import Chain


class ExplorerType(StrEnum):
    ETHERSCAN = "etherscan"
    CANTO = "canto"
    BLOCKSCOUT = "blockscout"
    SNOWTRACE = "snowtrace"
    BLOCKSCOUT_TRX_LIST_API = "blockscout-trx-list-api"
    BLOCKSCOUT_V5 = "blockscout-v5"
    HARMONY = "harmony"
    ZKSYNC = "zksync"

@dataclass
class ExplorerConfig:
    explorer_type: ExplorerType
    url: str
    min_seconds_between_requests: int = 10
    request_timeout: int = 10


EXPLORER_CONFIG: dict[Chain, ExplorerConfig] = {
    Chain.ARBITRUM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.arbiscan.io/api"),
    Chain.AURORA: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT_V5, url="https://old.explorer.aurora.dev/api"),
    Chain.AVAX: ExplorerConfig(explorer_type=ExplorerType.SNOWTRACE, url="https://snowtrace.dev/api"),
    Chain.BASE: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.basescan.org/api"),
    Chain.BSC: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.bscscan.com/api"),
    Chain.CANTO: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT_V5, url="https://explorer.plexnode.wtf/api"),
    Chain.CELO: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT, url="https://explorer.celo.org"),
    Chain.CRONOS: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.cronoscan.com/api"),
    Chain.EMERALD: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT, url="https://explorer.emerald.oasis.dev/"),
    Chain.ETHEREUM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.etherscan.io/api"),
    Chain.FANTOM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.ftmscan.com/api"),
    Chain.FUSE: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT_V5, url="https://explorer.fuse.io/api"),
    Chain.GNOSIS: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.gnosisscan.io/api"),
    Chain.HARMONY: ExplorerConfig(explorer_type=ExplorerType.HARMONY, url="https://explorer.harmony.one/"),
    Chain.HECO: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.hecoinfo.com/api"),
    Chain.KAVA: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT, url="https://kavascan.com"),
    Chain.LINEA: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT_TRX_LIST_API, url="https://explorer.linea.build"),
    Chain.METIS: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT, url="https://andromeda-explorer.metis.io"),
    Chain.MANTLE: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT, url="https://explorer.mantle.xyz", request_timeout=30),
    Chain.MOONBEAM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api-moonbeam.moonscan.io/api"),
    Chain.MOONRIVER: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api-moonriver.moonscan.io/api"),
    Chain.OPTIMISM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api-optimistic.etherscan.io/api"),
    Chain.POLYGON: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api.polygonscan.com/api"),
    Chain.ROLLUX: ExplorerConfig(explorer_type=ExplorerType.BLOCKSCOUT_TRX_LIST_API, url="https://explorer.rollux.com"),
    Chain.SCROLL: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://scrollscan.com"),
    Chain.ZKEVM: ExplorerConfig(explorer_type=ExplorerType.ETHERSCAN, url="https://api-zkevm.polygonscan.com/api"),
    Chain.ZKSYNC: ExplorerConfig(explorer_type=ExplorerType.ZKSYNC, url="https://block-explorer-api.mainnet.zksync.io"),
}
