import pathlib
import subprocess
import sys

import pytest
import yaml

from ..commandline.pack import pack

@pytest.mark.parametrize(
    "cli_args",
    [
        ["--version"],
        ["--list-catalogs"],
        ["--all"],
        ["--query", "{}"],
        ["--all", "--no-documents"],
        ["--all", "--strict"],
        ["--all", "--limit", "1"],
        ["--all", "--salt", "asdf"],
        ["--all", "--ignore-external"],
        ["--all", "--copy-external"],
        ["--all", "--fill-external"],
        [
            "--all",
            "--fill-external",
            "--handler-registry",
            "{'NPY_SEQ': 'ophyd.sim.NumpySeqHandler'}",
        ],
        # Repeat some of the above with JSONL instead of the default msgpack.
        # We could use a second layer of parametrize here but that seems more
        # confusing than helpful.
        ["--format", "jsonl", "--query", "{}"],
        ["--format", "jsonl", "--all", "--no-documents"],
        ["--format", "jsonl", "--all", "--strict"],
        ["--format", "jsonl", "--all", "--limit", "1"],
        ["--format", "jsonl", "--all", "--salt", "asdf"],
        ["--format", "jsonl", "--all", "--ignore-external"],
        ["--format", "jsonl", "--all", "--copy-external"],
        ["--format", "jsonl", "--all", "--fill-external"],
        [
            "--format",
            "jsonl",
            "--all",
            "--fill-external",
            "--handler-registry",
            "{'NPY_SEQ': 'ophyd.sim.NumpySeqHandler'}",
        ],
    ],
)
def test_pack_smoke(cli_args, simple_catalog, tmpdir):
    "Smoke test common options."
    TIMEOUT = 10
    CATALOG = simple_catalog
    pack([CATALOG, tmpdir, *cli_args])
