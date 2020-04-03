import collections
import hashlib
import logging
import os
import pathlib
import shutil

import event_model
import databroker.core
from tqdm import tqdm
import yaml

from ._version import get_versions

__all__ = (
    "export_uids",
    "export_catalog",
    "export_run",
    "write_external_files_manifest",
    "write_jsonl_catalog_file",
    "write_msgpack_catalog_file",
)
logger = logging.getLogger(__name__)


# Write through tqdm to avoid overlapping with bars.
def print(*args):
    tqdm.write(" ".join(str(arg) for arg in args))


def export_uids(
    source_catalog,
    uids,
    directory,
    *,
    strict=False,
    external=None,
    dry_run=False,
    handler_registry=None,
    serializer_class=None,
):
    """
    Export Runs from a Catalog, given a list of RunStart unique IDs.

    Parameters
    ----------
    source_catalog: Catalog
    uids: List[Str]
        List of RunStart unique IDs
    directory: Union[Str, Manager]
        Where files containing documents will be written, or a Manager for
        writing to non-file buffers.
    strict: Bool, optional
        By default, swallow erros and return a lits of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.
    handler_registry: Union[Dict, None]
        If None, automatic handler discovery is used.
    serializer_class: Serializer
        Expected to be a lossless serializer that encodes a format for which
        there is a corresponding databroker intake driver. Default (None) is
        currently ``suitcase.msgpack.Serializer``, but this may change in the
        future. If you want ``suitcase.msgpack.Serializer`` specifically, pass
        it in explicitly.

    Returns
    -------
    files: Dict[Str, Set[Str]]
        Maps each "root" to a set of absolute file paths.
    """
    accumulated_files = collections.defaultdict(set)
    failures = []
    with tqdm(total=len(uids), position=1, desc="Writing Documents") as progress:
        for uid in uids:
            try:
                run = source_catalog[uid]
                files = export_run(
                    run,
                    directory,
                    external=external,
                    dry_run=dry_run,
                    handler_registry=handler_registry,
                    root_map=source_catalog.root_map,
                    serializer_class=serializer_class,
                )
                for root, set_ in files.items():
                    accumulated_files[root].update(set_)
            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                print("FAILED:", uid)
            progress.update()
    return accumulated_files, failures


def export_catalog(
    source_catalog,
    directory,
    *,
    strict=False,
    external=None,
    dry_run=False,
    handler_registry=None,
    serializer_class=None,
):
    """
    Export all the Runs from a Catalog.

    Parameters
    ----------
    source_catalog: Catalog
    directory: Union[Str, Manager]
        Where files containing documents will be written, or a Manager for
        writing to non-file buffers.
    strict: Bool, optional
        By default, swallow erros and return a lits of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.
    handler_registry: Union[Dict, None]
        If None, automatic handler discovery is used.
    serializer_class: Serializer
        Expected to be a lossless serializer that encodes a format for which
        there is a corresponding databroker intake driver. Default (None) is
        currently ``suitcase.msgpack.Serializer``, but this may change in the
        future. If you want ``suitcase.msgpack.Serializer`` specifically, pass
        it in explicitly.

    Returns
    -------
    files: Dict[Str, Set[Str]]
        Maps each "root" to a set of absolute file paths.
    """
    accumulated_files = collections.defaultdict(set)
    failures = []
    with tqdm(total=len(source_catalog), position=1) as progress:
        for uid, run in source_catalog.items():
            try:
                files = export_run(
                    run,
                    directory,
                    external=external,
                    dry_run=dry_run,
                    handler_registry=handler_registry,
                    root_map=source_catalog.root_map,
                    serializer_class=serializer_class,
                )
                for root, set_ in files.items():
                    accumulated_files[root].update(set_)
            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                print("FAILED:", uid)
            progress.update()
    return dict(accumulated_files), failures


def export_run(
    run,
    directory,
    *,
    external=None,
    dry_run=False,
    handler_registry=None,
    root_map=None,
    serializer_class=None,
):
    """
    Export one Run.

    Parameters
    ----------
    run: BlueskyRun
    directory: Union[Str, Manager]
        Where files containing documents will be written, or a Manager for
        writing to non-file buffers.
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.
    handler_registry: Union[Dict, None]
        If None, automatic handler discovery is used.
    serializer_class: Serializer, optional
        Expected to be a lossless serializer that encodes a format for which
        there is a corresponding databroker intake driver. Default (None) is
        currently ``suitcase.msgpack.Serializer``, but this may change in the
        future. If you want ``suitcase.msgpack.Serializer`` specifically, pass
        it in explicitly.

    Returns
    -------
    files: Dict[Str, Set[Str]]
        Maps each "root" to a set of absolute file paths.
    """
    if serializer_class is None:
        import suitcase.msgpack

        serializer_class = suitcase.msgpack.Serializer
    root_map = root_map or {}
    resources = []
    files = collections.defaultdict(set)
    if handler_registry is None:
        handler_registry = databroker.core.discover_handlers()
    with event_model.Filler(
        handler_registry, inplace=False, root_map=root_map
    ) as filler:
        with serializer_class(directory) as serializer:
            with tqdm(position=0) as progress:
                for name, doc in run.canonical(fill="no"):
                    if name == "resource":
                        resources.append(doc)
                    if external == "fill":
                        name, doc = filler(name, doc)
                    if not dry_run:
                        serializer(name, doc)
                    progress.update()
        if external is None:
            for resource in resources:
                root = root_map.get(resource["root"], resource["root"])
                files[root].update(run.get_file_list(resource))
    return dict(files)


def _root_hash(root):
    # This is just a unique ID to give the manifest file for each
    # root a unique name. It is not a cryptographic hash.
    return hashlib.md5(root.encode()).hexdigest()


def write_external_files_manifest(manager, root, files):
    """
    Write a manifest of external files.

    Parameters
    ----------
    manager: suitcase Manager object
    root: Str
    files: Iterable[Union[Str, Path]]
    """
    root_hash = _root_hash(root)
    # The is the number of parts of the path that comprise the
    # root, so that we can reconstruct which part of the paths in
    # the file are the "root". (This information is available in
    # other ways, so putting it here is just a convenience.)
    # We subract one because we do not count '/'.
    # So the root_index of '/tmp/weoifjew' is 2.
    root_index = len(pathlib.Path(root).parts) - 1
    name = f"external_files_manifest_{root_hash}_{root_index}.txt"
    with manager.open("manifest", name, "xt") as file:
        file.write("\n".join(sorted((str(f) for f in files))))


def copy_external_files(target_directory, root, files):
    """
    Make a filesystem copy of the external files.

    A filesystem copy is not always applicable/desirable. Use the
    external_file_manifest_*.txt files to feed other file transfer mechanisms,
    such as rsync or globus.

    This is a wrapper around shutil.copy2.

    Parameters
    ----------
    target_directory: Union[Str, Path]
    root: Str
    files: Iterable[Str]

    Returns
    -------
    new_root, new_files
    """
    root_hash = _root_hash(root)
    dest = str(pathlib.Path(target_directory, root_hash))
    new_files = []
    for filename in tqdm(files, total=len(files), desc="Copying external files"):
        relative_path = pathlib.Path(filename).relative_to(root)
        new_root = target_directory / root_hash
        dest = new_root / relative_path
        os.makedirs(dest.parent, exist_ok=True)
        new_files.append(shutil.copy2(filename, dest))
    return new_root, new_files


def write_msgpack_catalog_file(manager, directory, paths, root_map):
    """
    Write a YAML file with configuration for an intake catalog.

    Parameters
    ----------
    manager: suitcase Manager object
    directory: Str
        Directory to which paths below are relative
    paths: Union[Str, List[Str]]
        Relative (s) of JSONL files encoding Documents.
    root_map: Dict
    """
    # Ideally, the drivers should be able to cope with relative paths,
    # interpreting them as relative to the Catalog file. This requires changes
    # to intake (I think) so as a short-term hack, we make the paths aboslute
    # here but note the relative paths in a separate place.
    abs_paths = [str(pathlib.Path(directory, path).absolute()) for path in paths]
    metadata = {
        "generated_by": {
            "library": "databroker_pack",
            "version": get_versions()["version"],
        },
        "relative_paths": [str(path) for path in paths],
    }
    source = {
        "driver": "bluesky-msgpack-catalog",
        "args": {"paths": abs_paths},
        "metadata": metadata,
    }
    if root_map is not None:
        source["args"]["root_map"] = {str(k): str(v) for k, v in root_map.items()}
    sources = {"packed_catalog": source}
    catalog = {"sources": sources}
    FILENAME = "catalog.yml"  # expected by unpack
    with manager.open("catalog_file", FILENAME, "xt") as file:
        yaml.dump(catalog, file)


def write_jsonl_catalog_file(manager, directory, paths, root_map):
    """
    Write a YAML file with configuration for an intake catalog.

    Parameters
    ----------
    manager: suitcase Manager object
    directory: Str
        Directory to which paths below are relative
    paths: Union[Str, List[Str]]
        Relative (s) of JSONL files encoding Documents.
    root_map: Dict
    """
    # There is clearly some code repetition here with respect to
    # write_msgpack_catalog_file, but I expect they may diverge over time as
    # the suitcase implementation pick up format-specific options.

    # Ideally, the drivers should be able to cope with relative paths,
    # interpreting them as relative to the Catalog file. This requires changes
    # to intake (I think) so as a short-term hack, we make the paths aboslute
    # here but note the relative paths in a separate place.
    abs_paths = [str(pathlib.Path(directory, path).absolute()) for path in paths]
    metadata = {
        "generated_by": {
            "library": "databroker_pack",
            "version": get_versions()["version"],
        },
        "relative_paths": [str(path) for path in paths],
    }
    source = {
        "driver": "bluesky-jsonl-catalog",
        "args": {"paths": abs_paths},
        "metadata": metadata,
    }
    if root_map is not None:
        source["args"]["root_map"] = {str(k): str(v) for k, v in root_map.items()}
    sources = {"packed_catalog": source}
    catalog = {"sources": sources}
    FILENAME = "catalog.yml"
    with manager.open("catalog_file", FILENAME, "xt") as file:
        yaml.dump(catalog, file)
