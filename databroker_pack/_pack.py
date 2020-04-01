import suitcase.msgpack
from tqdm import tqdm
import logging

__all__ = ("export_uids", "export_catalog", "export_run")
logger = logging.getLogger(__name__)


# Write through tqdm to avoid overlapping with bars.
def print(*args):
    tqdm.write(" ".join(str(arg) for arg in args))


def export_uids(
    source_catalog, uids, directory, *, strict=False, omit_external=False, dry_run=False
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
    omit_external: Bool, optional
        If True, do not include any external files in the list of relevant
        files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.

    Returns
    -------
    files: List[Str]
        List of filepaths to msgpack files written and any external files.
    """
    accumulated_files = []
    failures = []
    with tqdm(total=len(uids), position=1) as progress:
        for uid in uids:
            try:
                run = source_catalog[uid]
                files = export_run(
                    run, directory, omit_external=omit_external, dry_run=dry_run
                )
                accumulated_files.extend(files)
            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                print("FAILED:", uid)
            progress.update()
    return accumulated_files, failures


def export_catalog(
    source_catalog, directory, *, strict=False, omit_external=False, dry_run=False
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
    omit_external: Bool, optional
        If True, do not include any external files in the list of relevant
        files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.

    Returns
    -------
    files: List[Str]
        List of filepaths to msgpack files written and any external files.
    """
    accumulated_files = []
    failures = []
    with tqdm(total=len(source_catalog), position=1) as progress:
        for uid, run in source_catalog.items():
            try:
                files = export_run(
                    run, directory, omit_external=omit_external, dry_run=dry_run
                )
                accumulated_files.extend(files)
            except Exception:
                logger.exception("Error while exporting Run %r", uid)
                if strict:
                    raise
                failures.append(uid)
                print("FAILED:", uid)
            progress.update()
    return accumulated_files, failures


def export_run(run, directory, *, omit_external=False, dry_run=False):
    """
    Export one Run.

    Parameters
    ----------
    run: BlueskyRun
    directory: Str
        Where msgpack files containing documents will be written
    omit_external: Bool, optional
        If True, do not include any external files in the list of relevant
        files.
    dry_run: Bool, optional
        If True, do not write any files. False by default.

    Returns
    -------
    files: List[Str]
        List of filepaths to msgpack files written and any external files.
    """
    resources = []
    files = set()
    with suitcase.msgpack.Serializer(directory) as serializer:
        with tqdm(position=0) as progress:
            for name, doc in run.canonical(fill="no"):
                if name == "resource":
                    resources.append(doc)
                if not dry_run:
                    serializer(name, doc)
                progress.update()
    if not omit_external:
        for resource in resources:
            files.update(run.get_file_list(resource))
    return sorted(files)
