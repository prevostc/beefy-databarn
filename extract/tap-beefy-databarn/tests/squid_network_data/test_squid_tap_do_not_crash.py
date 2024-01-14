from singer_sdk.testing import get_tap_test_class

from tap_beefy_databarn.tap import TapSquidContractEvents

SAMPLE_CONFIG = {"postgres_connection_string": "postgresql://beefy:beefy@localhost:5432/beefy"}


# Run standard built-in tap tests from the SDK:
TestTapBeefyDatabarn = get_tap_test_class(
    tap_class=TapSquidContractEvents,
    config=SAMPLE_CONFIG,
)
