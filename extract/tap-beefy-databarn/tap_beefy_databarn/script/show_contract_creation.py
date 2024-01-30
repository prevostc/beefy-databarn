import logging

import click

from tap_beefy_databarn.common.chains import Chain
from tap_beefy_databarn.contract_creation.block_explorer_client import BlockExplorerClient
from tap_beefy_databarn.contract_creation.contract_creation_models import ContractWatch

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

@click.command()
@click.option("--chain", "-c", required=True, type=Chain)
@click.option("--contract-address", "-a", required=True, type=str)
def show_contract_creation(chain: Chain, contract_address: str) -> None:
    client = BlockExplorerClient.from_chain(logger, chain)
    contract_watch = ContractWatch(chain=chain, contract_address=contract_address)
    logger.info("Contract watch: %s", contract_watch)

    # call multiple times to see if the rate limiter holds it's promises
    for contract_creation in client.get_contract_creation_infos([contract_watch] * 5):
        logger.info(contract_creation)
