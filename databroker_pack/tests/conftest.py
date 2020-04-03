import os
import pathlib

import databroker
import pytest

from .. import unpack

DATA_DIR = pathlib.Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def simple_catalog():
    DIRECTORY = "simple_catalog"
    NAME = "databroker_pack_tests_simple_catalog"
    config_path = unpack(DATA_DIR / DIRECTORY, NAME)
    try:
        # Use this once list_configs() is fixed to be complete.
        # It is incomplete in databroker 1.0.0.
        # assert NAME in databroker.utils.list_configs()
        databroker.catalog.force_reload()
        assert NAME in list(databroker.catalog)
        yield NAME
    finally:
        os.unlink(config_path)
