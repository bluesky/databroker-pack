import pathlib
import subprocess
import sys

import pytest
import yaml


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
@pytest.mark.parametrize("relative_target_directory", [True, False])
def test_pack_smoke(cli_args, simple_catalog, tmpdir, relative_target_directory):
    "Smoke test common options."
    TIMEOUT = 10
    CATALOG = simple_catalog
    if relative_target_directory:
        p = subprocess.Popen(
            [
                sys.executable,
                "-um",
                "databroker_pack.commandline.pack",
                CATALOG,
                pathlib.Path(tmpdir).parts[-1],
                *cli_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(pathlib.Path(tmpdir).parent),
        )
    else:
        p = subprocess.Popen(
            [
                sys.executable,
                "-um",
                "databroker_pack.commandline.pack",
                CATALOG,
                tmpdir,
                *cli_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    # Capture stdout, stderr for interactive debugging.
    stdout, stderr = p.communicate(TIMEOUT)
    assert p.returncode == 0


HASH = "3e3b48cf1579c708c4b515de4927d03a"  # stable because we give --salt SALT
REL_PATH = str(pathlib.Path("external_files", HASH))
ORIGINAL_ROOT = "f8483b80395561ac779a5425da6646ca"
ABS_PATH = str(
    pathlib.Path(__file__).parent
    / "data"
    / "simple_catalog"
    / "external_files"
    / ORIGINAL_ROOT
)


@pytest.mark.parametrize(
    "cli_args, expected",
    [
        ([], {HASH: ABS_PATH}),
        (["--no-documents"], {ORIGINAL_ROOT: ABS_PATH}),
        (["--copy-external"], {HASH: REL_PATH}),
        (["--no-documents", "--copy-external"], {ORIGINAL_ROOT: REL_PATH}),
    ],
    ids=[
        "hash_to_abs_path",
        "original_root_to_abs_path",
        "hash_to_rel_path",
        "original_root_to_rel_path",
    ],
)
@pytest.mark.parametrize("relative_target_directory", [True, False])
def test_root_map(
    cli_args, expected, simple_catalog, tmpdir, relative_target_directory
):
    "Smoke test common options."
    TIMEOUT = 10
    CATALOG = simple_catalog
    if relative_target_directory:
        p = subprocess.Popen(
            [
                sys.executable,
                "-um",
                "databroker_pack.commandline.pack",
                CATALOG,
                pathlib.Path(tmpdir).parts[-1],
                "--all",
                "--salt",
                "SALT",
                *cli_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(pathlib.Path(tmpdir).parent),
        )
    else:
        p = subprocess.Popen(
            [
                sys.executable,
                "-um",
                "databroker_pack.commandline.pack",
                CATALOG,
                tmpdir,
                "--all",
                "--salt",
                "SALT",
                *cli_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    # Capture stdout, stderr for interactive debugging.
    stdout, stderr = p.communicate(TIMEOUT)
    assert p.returncode == 0
    with open(pathlib.Path(tmpdir, "catalog.yml")) as file:
        catalog = yaml.safe_load(file)
    root_map = catalog["sources"]["packed_catalog"]["args"]["root_map"]
    assert root_map == expected
