import os
import pathlib

import pytest

from .._pack import copy_external_files, _root_hash


def test_strict_copy_external_files(tmpdir):
    root = str(pathlib.Path(tmpdir, "source"))
    os.makedirs(root)
    target_directory = str(pathlib.Path(tmpdir, "dest"))
    filepath = pathlib.Path(root, "testfile")
    with open(filepath, "w") as file:
        file.write("placeholder")

    # Test a successful copy.
    _, _, failures = copy_external_files(
        target_directory, root, [pathlib.Path(root, "testfile")], strict=True
    )
    assert not failures
    with open(pathlib.Path(target_directory, _root_hash(root), "testfile")) as file:
        assert file.read() == "placeholder"

    # Test failures with and without strict.
    nonexistant_file = pathlib.Path(root, _root_hash(root), "DOES_NOT_EXIST")
    with pytest.raises(FileNotFoundError):
        copy_external_files(
            target_directory, root, [nonexistant_file], strict=True,
        )
    _, _, failures = copy_external_files(
        target_directory, root, [nonexistant_file], strict=False,
    )
    assert failures == [nonexistant_file]
