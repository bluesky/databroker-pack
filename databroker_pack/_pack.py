import collections
import functools
import logging
import os
import pathlib
import secrets
import shutil

import event_model
import databroker.core
from tqdm import tqdm
import yaml

from ._utils import root_hash
from ._version import get_versions

__all__ = (
    "copy_external_files",
    "export_uids",
    "export_catalog",
    "export_run",
    "write_documents_manifest",
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
    no_documents=False,
    handler_registry=None,
    serializer_class=None,
    salt=None,
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
        By default, swallow erros and return a list of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'ignore')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'ignore', do not locate external files.
    no_documents: Bool, optional
        If True, do not write any files. False by default.
    handler_registry: Union[Dict, None]
        If None, automatic handler discovery is used.
    serializer_class: Serializer
        Expected to be a lossless serializer that encodes a format for which
        there is a corresponding databroker intake driver. Default (None) is
        currently ``suitcase.msgpack.Serializer``, but this may change in the
        future. If you want ``suitcase.msgpack.Serializer`` specifically, pass
        it in explicitly.
    salt: Union[bytes, None]
        We want to make hashes is unique to:

        - a root
        - a given batch of exported runs (i.e., a given call to this function)

        so that we can use it as a key in root_map which is guaranteed not to
        collide with keys from other batches. Thus, we create a "salt" unless
        one is specified here. This does not need to be cryptographically
        secure, just unique.

    Returns
    -------
    artifacts, files, failures

    Notes
    -----
    * ``artifacts`` maps a human-readable string (typically just ``'all'`` in
      this case) to a list of buffers or filepaths where the documents were
      serialized.
    * ``files`` is the set of filepaths of all external files referenced by
      Resource documents, keyed on ``(root_in_document, root, unique_id)``.
    * ``failures`` is a list of uids of runs that raised Exceptions. (The
      relevant tracebacks are logged.)
    """
    accumulated_files = collections.defaultdict(set)
    accumulated_artifacts = collections.defaultdict(set)
    failures = []
    if salt is None:
        salt = secrets.token_hex(32).encode()
    root_hash_func = functools.partial(root_hash, salt)
    with tqdm(total=len(uids), position=1, desc="Writing Documents") as progress:
        for uid in uids:
            try:
                run = source_catalog[uid]
                artifacts, files = export_run(
                    run,
                    directory,
                    root_hash_func,
                    external=external,
                    no_documents=no_documents,
                    handler_registry=handler_registry,
                    root_map=source_catalog.root_map,
                    serializer_class=serializer_class,
                )
                for root, set_ in files.items():
                    accumulated_files[root].update(set_)
                for name, list_ in artifacts.items():
                    accumulated_artifacts[name].update(list_)

            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                progress.set_description(
                    f"Writing Documents ({len(failures)} failures)", refresh=False
                )
            progress.update()
    return dict(accumulated_artifacts), dict(accumulated_files), failures


def export_catalog(
    source_catalog,
    directory,
    *,
    strict=False,
    external=None,
    no_documents=False,
    handler_registry=None,
    serializer_class=None,
    salt=None,
    limit=None,
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
        By default, swallow erros and return a list of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'ignore')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'ignore', do not locate external files.
    no_documents: Bool, optional
        If True, do not serialize documents. False by default.
    handler_registry: Union[Dict, None]
        If None, automatic handler discovery is used.
    serializer_class: Serializer
        Expected to be a lossless serializer that encodes a format for which
        there is a corresponding databroker intake driver. Default (None) is
        currently ``suitcase.msgpack.Serializer``, but this may change in the
        future. If you want ``suitcase.msgpack.Serializer`` specifically, pass
        it in explicitly.
    salt: Union[bytes, None]
        We want to make hashes is unique to:

        - a root
        - a given batch of exported runs (i.e., a given call to this function)

        so that we can use it as a key in root_map which is guaranteed not to
        collide with keys from other batches. Thus, we create a "salt" unless
        one is specified here. This does not need to be cryptographically
        secure, just unique.
    limit: Union[Integer, None]
        Stop after exporting some number of Runs. Useful for testing a subset
        before doing a lengthy export.

    Returns
    -------
    artifacts, files, failures

    Notes
    -----
    * ``artifacts`` maps a human-readable string (typically just ``'all'`` in
      this case) to a list of buffers or filepaths where the documents were
      serialized.
    * ``files`` is the set of filepaths of all external files referenced by
      Resource documents, keyed on ``(root_in_document, root, unique_id)``.
    * ``failures`` is a list of uids of runs that raised Exceptions. (The
      relevant tracebacks are logged.)
    """
    if limit is not None:
        if limit < 1:
            raise ValueError("limit must be None or a number 1 or greater")
        limit = int(limit)
    accumulated_files = collections.defaultdict(set)
    accumulated_artifacts = collections.defaultdict(set)
    failures = []
    if salt is None:
        salt = secrets.token_hex(32).encode()
    root_hash_func = functools.partial(root_hash, salt)
    with tqdm(
        total=limit or len(source_catalog), position=1, desc="Writing Documents"
    ) as progress:
        for i, (uid, run) in enumerate(source_catalog.items()):
            if i == limit:
                break
            try:
                artifacts, files = export_run(
                    run,
                    directory,
                    root_hash_func,
                    external=external,
                    no_documents=no_documents,
                    handler_registry=handler_registry,
                    root_map=source_catalog.root_map,
                    serializer_class=serializer_class,
                )
                for root, set_ in files.items():
                    accumulated_files[root].update(set_)
                for name, list_ in artifacts.items():
                    accumulated_artifacts[name].update(list_)
            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                progress.set_description(
                    f"Writing Documents ({len(failures)} failures)", refresh=False
                )
            progress.update()
    return dict(accumulated_artifacts), dict(accumulated_files), failures


def export_run(
    run,
    directory,
    root_hash_func,
    *,
    external=None,
    no_documents=False,
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
    external: {None, 'fill', 'ignore')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'ignore', do not locate external files.
    no_documents: Bool, optional
        If True, do not serialize documents. False by default.
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
    artifacts, files

    Notes
    -----
    * ``artifacts`` maps a human-readable string (typically just ``'all'`` in
      this case) to a list of buffers or filepaths where the documents were
      serialized.
    * ``files`` is the set of filepaths of all external files referenced by
      Resource documents, keyed on ``(root_in_document, root, unique_id)``.
    """
    EXTERNAL_RELATED_DOCS = ("resource", "datum", "datum_page")
    if serializer_class is None:
        import suitcase.msgpack

        serializer_class = suitcase.msgpack.Serializer
    root_map = root_map or {}
    files = collections.defaultdict(set)
    if handler_registry is None:
        handler_registry = databroker.core.discover_handlers()
    with event_model.Filler(
        handler_registry, inplace=False, root_map=root_map
    ) as filler:
        with serializer_class(
            directory, file_prefix="documents/{start[uid]}"
        ) as serializer:
            with tqdm(position=0) as progress:
                for name, doc in run.canonical(fill="no"):
                    if external == "fill":
                        name, doc = filler(name, doc)
                        # Omit Resource and Datum[Page] because the data was
                        # filled in place.
                        if name in EXTERNAL_RELATED_DOCS:
                            progress.update()
                            continue
                    elif name == "resource":
                        root = root_map.get(doc["root"], doc["root"])
                        unique_id = root_hash_func(doc["root"])
                        if external is None:
                            if no_documents:
                                root_in_document = doc["root"]
                            else:
                                root_in_document = root
                            # - root_in_document is the 'root' actually in the
                            # resource_document
                            # - root may be different depending on the
                            # source_catalog configuration, which can map the
                            # recorded 'root' in the document to some other
                            # location. This is where we should go looking for
                            # the data if we plan to copy it.
                            # - unique_id is unique to this (root, salt)
                            # combination and used to place the data in a
                            # unique location.
                            key = (root_in_document, root, unique_id)
                            files[key].update(run.get_file_list(doc))
                        if not no_documents:
                            # Replace root with a unique ID before serialization.
                            # We are overriding the local variable name doc here
                            # (yuck!) so that serializer(name, doc) below works on
                            # all document types.
                            doc = doc.copy()
                            doc["root"] = unique_id
                    if not no_documents:
                        serializer(name, doc)
                    progress.update()
    return serializer.artifacts, dict(files)


def write_external_files_manifest(manager, unique_id, files):
    """
    Write a manifest of external files.

    Parameters
    ----------
    manager: suitcase Manager object
    unique_id: Str
    files: Iterable[Union[Str, Path]]
    """
    name = f"external_files_manifest_{unique_id}.txt"
    with manager.open("manifest", name, "xt") as file:
        file.write("\n".join(sorted((str(f) for f in set(files)))))


def copy_external_files(target_directory, root, unique_id, files, strict=False):
    """
    Make a filesystem copy of the external files.

    A filesystem copy is not always applicable/desirable. Use the
    external_file_manifest_*.txt files to feed other file transfer mechanisms,
    such as rsync or globus.

    This is a wrapper around shutil.copyfile.

    Parameters
    ----------
    target_directory: Union[Str, Path]
    root: Str
    files: Iterable[Str]
    strict: Bool, optional
        By default, swallow erros and return a list of them at the end.
        Set to True to debug errors.

    Returns
    -------
    new_root, new_files, failures

    Notes
    -----
    * ``new_root`` is a Path to the new root directory
    * ``new_files`` is the list of filepaths to the files that were created.
    * ``failures`` is a list of uids of runs that raised Exceptions. (The
      relevant tracebacks are logged.)
    """
    new_files = []
    failures = []
    for filename in tqdm(files, total=len(files), desc="Copying external files"):
        relative_path = pathlib.Path(filename).relative_to(root)
        new_root = pathlib.Path(target_directory, unique_id)
        dest = new_root / relative_path
        try:
            os.makedirs(dest.parent, exist_ok=True)
            new_files.append(shutil.copyfile(filename, dest))
        except Exception:
            logger.exception("Error while copying %r to %r", filename, dest)
            if strict:
                raise
            failures.append(filename)
    return new_root, new_files, failures


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


def write_documents_manifest(manager, directory, artifacts):
    """
    Wirte the paths to all the files of Documents relative to the pack directory.

    Parameters
    ----------
    manager: suitcase Manager object
    directory: Str
        Pack directory
    artifacts: List[Str]
    """
    FILENAME = "documents_manifest.txt"
    abs_directory = pathlib.Path(directory).absolute()
    with manager.open("documents_manifest", FILENAME, "xt") as file:
        for artifact in artifacts:
            file.write(f"{pathlib.Path(artifact).relative_to(abs_directory)!s}\n")
