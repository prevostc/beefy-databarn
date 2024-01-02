import logging

import dotenv

from extract.common.config_util import EnvParser

dotenv.load_dotenv(interpolate=False, override=True)

env = EnvParser()

# The log level to use
LOG_LEVEL = env.get(name="LOG_LEVEL", to_type=int) or logging.DEBUG

POSTGRES_CONNECTION_STRING = env.get(name="POSTGRES_CONNECTION_STRING", to_type=str) or "postgresql://postgres:postgres@localhost:5432/postgres"
