import os
from pathlib import PurePath
from pkg_resources import resource_filename

import databroker
import pytest

from databroker_pack._unpack import unpack_inplace

DATA_DIR = PurePath(resource_filename('databroker_pack', 'tests/data'))


@pytest.fixture(scope="module")
def simple_catalog():
    DIRECTORY = "simple_catalog"
    NAME = "databroker_pack_tests_simple_catalog"
    config_path = unpack_inplace(str(DATA_DIR.joinpath(DIRECTORY)), NAME)
    try:
        # Use this once list_configs() is fixed to be complete.
        # It is incomplete in databroker 1.0.0.
        # assert NAME in databroker.utils.list_configs()
        databroker.catalog.force_reload()
        assert NAME in list(databroker.catalog)
        yield NAME
    finally:
        os.unlink(config_path)
