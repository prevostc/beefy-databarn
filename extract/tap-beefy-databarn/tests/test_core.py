"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class
from tap_beefy_databarn.tap import TapBeefyDatabarn

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d"),
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
TestTapBeefyDatabarn = get_tap_test_class(
    tap_class=TapBeefyDatabarn,
    config=SAMPLE_CONFIG,
)


# TODO: Create additional tests as appropriate for your tap.
