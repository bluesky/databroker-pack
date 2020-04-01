import collections
import logging

import event_model
import databroker.core
import suitcase.msgpack
from tqdm import tqdm

__all__ = ("export_uids", "export_catalog", "export_run")
logger = logging.getLogger(__name__)


# Write through tqdm to avoid overlapping with bars.
def print(*args):
    tqdm.write(" ".join(str(arg) for arg in args))


def export_uids(
    source_catalog, uids, directory, *, strict=False, external=None, dry_run=False
):
    """
    Export Runs from a Catalog, given a list of RunStart unique IDs.

    Parameters
    ----------
    source_catalog: Catalog
    uids: List[Str]
        List of RunStart unique IDs
    directory: Str
        Where msgpack files containing documents will be written
    strict: Bool, optional
        By default, swallow erros and return a lits of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.

    Returns
    -------
    files: Dict[Str, Set[Str]]
        Maps each "root" to a set of absolute file paths.
    """
    accumulated_files = collections.defaultdict(set)
    failures = []
    with tqdm(total=len(uids), position=1) as progress:
        for uid in uids:
            try:
                run = source_catalog[uid]
                files = export_run(run, directory, external=external, dry_run=dry_run)
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
    source_catalog, directory, *, strict=False, external=None, dry_run=False
):
    """
    Export all the Runs from a Catalog.

    Parameters
    ----------
    source_catalog: Catalog
    directory: Str
        Where msgpack files containing documents will be written
    strict: Bool, optional
        By default, swallow erros and return a lits of them at the end.
        Set to True to debug errors.
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.

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
                files = export_run(run, directory, external=external, dry_run=dry_run)
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


def export_run(run, directory, *, external=None, dry_run=False, handler_registry=None):
    """
    Export one Run.

    Parameters
    ----------
    run: BlueskyRun
    directory: Str
        Where msgpack files containing documents will be written
    external: {None, 'fill', 'omit')
        If None, return the paths to external files.
        If 'fill', fill the external data into the Documents.
        If 'omit', do not locate external files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.
    handler_registry: Union[Dict, None]

    Returns
    -------
    files: Dict[Str, Set[Str]]
        Maps each "root" to a set of absolute file paths.
    """
    resources = []
    files = collections.defaultdict(set)
    if handler_registry is None:
        handler_registry = databroker.core.discover_handlers()
    with event_model.Filler(handler_registry, inplace=False) as filler:
        with suitcase.msgpack.Serializer(directory) as serializer:
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
                files[resource["root"]].update(run.get_file_list(resource))
    return dict(files)
