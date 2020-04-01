#!/usr/bin/env python
import argparse
import functools
import hashlib
import logging
import pathlib
import sys
import tempfile

from suitcase.utils import MultiFileManager

from ._utils import ListCatalogsAction, ShowVersionAction
from .._pack import export_catalog, export_uids


MANIFEST_NAME_TEMPLATE = "external_files_manifest_{root_hash}_{root_index}.txt"
print = functools.partial(print, file=sys.stderr)


def main():
    with tempfile.NamedTemporaryFile("w", delete=False) as file:
        error_logfile_name = file.name
    error_handler = logging.FileHandler(error_logfile_name)
    logging.getLogger("databroker_pack").addHandler(error_handler)
    parser = argparse.ArgumentParser(
        description="Pack up some Bluesky Runs into portable files.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
*~ Examples ~*

List the available options for CATALOG and exit.

$ databroker-pack --list-catalogs
...

Export Runs from a range of time.

$ databroker-pack CATALOG -q "TimeRange(since='2020')" DIRECTORY
$ databroker-pack CATALOG -q "TimeRange(since='2020', until='2020-03-01)" DIRECTORY

Export Runs from a range of time with a certain plan_name.

$ databroker-pack CATALOG -q "TimeRange(since='2020')" -q "{'plan_name': 'count'}" DIRECTORY

Export a specific Run by its scan_id

$ databroker-pack CATALOG -q "{'scan_id': 126360}" DIRECTORY

Export specific Runs given by their Run Start UID (or the first several
characters) entered at the command prompt...

$ databroker-pack CATALOG --uids -
3c93c54e
47587fa8
ebad8c01
<Ctrl D>

...or read from a file.

$ databroker-pack CATALOG --uids uids_to_pack.txt

Export an entire catalog.

$ databroker-pack CATALOG --all DIRECTORY

Copy the external data files into the output directory.

$ databroker-pack CATALOG --all --copy-external DIRECTORY
""",
    )
    parser.register("action", "list_catalogs", ListCatalogsAction)
    parser.register("action", "show_version", ShowVersionAction)
    filter_group = parser.add_argument_group(
        description="Which Runs should we pack? Must specify one of these:"
    ).add_mutually_exclusive_group()
    external_group = parser.add_argument_group(
        description=(
            "What should we do with external files (e.g. large array data "
            "files written by detectors)? By default, we will write a text "
            "file with all the relevant file paths. Other options:"
        )
    ).add_mutually_exclusive_group()
    parser.add_argument("catalog", type=str, help="Catalog name")
    parser.add_argument("directory", type=str, help="Path to output directory")
    parser.add_argument(
        "--list-catalogs",
        action="list_catalogs",
        default=argparse.SUPPRESS,
        help="List allowed values for catalog and exit.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="show_version",
        default=argparse.SUPPRESS,
        help="Show databroker_pack version and exit.",
    )
    filter_group.add_argument(
        "--all", action="store_true", help="Export every Run in this Catalog."
    )
    filter_group.add_argument(
        "-q",
        "--query",
        type=str,
        action="append",
        help=(
            "MongoDB-style query or databroker.queries Query. "
            "Narrow results by chaining multiple queries like "
            "-q \"TimeRange(since='2020')\" "
            "-q \"{'sample': 'Au'}\""
        ),
    )
    filter_group.add_argument(
        "--uids",
        type=argparse.FileType("r"),
        action="append",
        help=("Newline-separated (partial) uids. Lines starting with # are skipped."),
    )
    external_group.add_argument(
        "--copy-external",
        action="store_true",
        help="Copy relevant external files into the output directory.",
    )
    external_group.add_argument(
        "--fill-external",
        action="store_true",
        help="Place external data directly in the documents.",
    )
    external_group.add_argument(
        "--no-manifests",
        action="store_true",
        help=(
            "By default, the locations of all relevant external files on the "
            f"source machine are written to text files. "
            "Set this to omit those manifests."
        ),
    )
    parser.add_argument(
        "--no-documents", action="store_true", help="Do not pack the Documents.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Exit when error occurs. Otherwise failures are logged to "
            "stderr as they happen "
            "and again all together at the end."
        ),
    )
    args = parser.parse_args()
    if args.no_manifests:
        external = "omit"
    elif args.fill_external:
        external = "fill"
    else:
        external = None
    import databroker
    import databroker.queries

    catalog = databroker.catalog[args.catalog]()
    manager = MultiFileManager(args.directory)
    # Make a separate manager instance in order to allow appending to the
    # manifest (but not in general).
    manifest_manager = MultiFileManager(args.directory, allowed_modes="a")
    try:
        if args.query or args.all:
            if args.all:
                # --all is an alias for --query "{}"
                raw_queries = ["{}"]
            else:
                raw_queries = args.query
            queries = []
            for raw_query in raw_queries:
                # Parse string like "{'scan_id': 123}" to dict.
                try:
                    query = dict(eval(raw_query, vars(databroker.queries)))
                except Exception:
                    raise ValueError("Could not parse query {raw_query}.")
                queries.append(query)
            combined_query = {"$and": queries}
            # HACK We need no_cursor_timeout only until databroker learns to
            # seamlessly remake cursors when they time out.
            results = catalog.search(combined_query, no_cursor_timeout=True)
            if not results:
                sys.exit(1)
            external_files, failures = export_catalog(
                results,
                manager,
                strict=args.strict,
                external=external,
                dry_run=args.no_documents,
            )
        elif args.uids:
            # Skip blank lines and commented lines.
            uids = []
            for uid_file in args.uids:
                uids.extend(
                    line.strip()
                    for line in uid_file.read().splitlines()
                    if line and not line.startswith("#")
                )
            print(f"Found {len(uids)} uids.")
            if not uids:
                sys.exit(1)
                external_files, failures = export_uids(
                    catalog,
                    uids,
                    manager,
                    strict=args.strict,
                    external=external,
                    dry_run=args.no_documents,
                )
        else:
            parser.error(
                "Must specify which Runs to pack, --query ... or "
                "--uids ... or --all."
            )
        if not args.no_manifests:
            for root, files in external_files.items():
                if not files:
                    # fast path
                    continue
                # This is just a unique ID to give the manifest file for each
                # root a unique name. It is not a cryptographic hash.
                root_hash = hashlib.md5(root.encode()).hexdigest()
                # The is the number of parts of the path that comprise the
                # root, so that we can reconstruct which part of the paths in
                # the file are the "root". (This information is available in
                # other ways, so putting it here is just a convenience.)
                # We subract one because we do not count '/'.
                # So the root_index of '/tmp/weoifjew' is 2.
                root_index = len(pathlib.Path(root).parts - 1)
                name = MANIFEST_NAME_TEMPLATE.format(
                    root_hash=root_hash, root_index=root_index
                )
                with manifest_manager.open("manifest", name, "a") as file:
                    # IF we are appending to a nonempty file, ensure we start
                    # on a new line.
                    if file.tell():
                        file.write("\n")
                    file.write("\n".join(sorted(files)))
        if failures:
            print(f"{len(failures)} Runs failed to pack.")
            with tempfile.NamedTemporaryFile("w", delete=False) as file:
                print(f"See {file.name} for a list of uids of Runs that failed.")
                file.write("\n".join(failures))
            print(f"See {error_logfile_name} for error logs with more information.")
            sys.exit(1)
    finally:
        manager.close()
        manifest_manager.close()
