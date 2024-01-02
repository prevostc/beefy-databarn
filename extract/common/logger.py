import logging
import sys

from extract.common.config import LOG_LEVEL

# configure root logger
logging.basicConfig(level=LOG_LEVEL)
logging.root.setLevel(LOG_LEVEL)


# configure our app logger
logger = logging.getLogger("beefy-databarn")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(LOG_LEVEL)

logger.debug("Logger initialized with level %s", LOG_LEVEL)
