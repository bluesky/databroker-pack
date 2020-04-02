import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "cli_args",
    [
        ["--all'"],
        ['--query  "{}"'],
        ["--version"],
        ["--list-catalogs"],
        ["--all", "--no-documents"],
        ["--all", "--strict"],
        ["--all", "--no-manifests"],
        ["--all", "--copy-files"],
        ["--all", "--fill-external"],
    ],
)
def test_smoke(cli_args, tmpdir):
    "Smoke test common options."
    TIMEOUT = 10
    DIRECTORY = tmpdir
    CATALOG = ...
    p = subprocess.Popen(
        [sys.executable, "-um", "databroker_pack.commandline.pack"]
        + [CATALOG, DIRECTORY]
        + cli_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Capture stdout, stderr for interactive debugging.
    stdout, stderr = p.communicate(TIMEOUT)
    assert p.returncode == 0
