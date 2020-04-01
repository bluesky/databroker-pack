#!/usr/bin/env python
import argparse
import tempfile
import sys

from suitcase.utils import MultiFileManager

from ._utils import ShowVersionAction
from .._pack import export_catalog, export_uids


MANIFEST_FILE_NAME = "external_files_manifest.txt"


def main():
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
    parser.register("action", "list_catalogs", _ListCatalogsAction)
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
        "--no-manifest",
        action="store_true",
        help=(
            "By default, the locations of all relevant external files on the "
            f"source machine are written to a text file named {MANIFEST_FILE_NAME}. "
            "Set this to omit that manifest. Building the manifest "
            "may require accessing the relevant files (depending on the "
            "of the file format). If the external files are not needed and "
            "cannot be accessed, you may invoke this option to export only "
            "the content of the Documents, skipping the manifest."
        ),
    )
    parser.add_argument(
        "--no-documents", action="store_true", help="Do not pack the Documents.",
    )
    # TODO Do we want to support this? It would be easy to misuse.
    # external_group.add_argument(
    #     "--fill-external-data,"
    #     action="store_true",
    #     help="Place external data directly in the documents.")
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
    import databroker
    import databroker.queries

    catalog = databroker.catalog[args.catalog]()
    manager = MultiFileManager(args.directory)
    # Make a separate manager instance in order to allow appending to the
    # manifest (but not in general).
    another_manager = MultiFileManager(args.directory, allowed_modes="a")
    try:
        if args.query:
            queries = []
            for raw_query in args.query:
                # Parse string like "{'scan_id': 123}" to dict.
                try:
                    query = dict(eval(raw_query, vars(databroker.queries)))
                except Exception:
                    raise ValueError("Could not parse query {raw_query}.")
                queries.append(query)
            combined_query = {"$and": queries}
            print(f"Query parsed as {combined_query!r}")
            # HACK We need no_cursor_timeout only until databroker learns to
            # seamlessly remake cursors when they time out.
            results = catalog.search(combined_query, no_cursor_timeout=True)
            print(f"Query yielded {len(results)} result(s)")
            if not results:
                sys.exit(1)
            if not args.no_documents:
                print(f"Writing documents....")
                external_files, failures = export_catalog(
                    results, manager, strict=args.strict, omit_external=args.no_manifest
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
            if not args.no_documents:
                print(f"Writing documents....")
                external_files, failures = export_uids(
                    catalog,
                    uids,
                    manager,
                    strict=args.strict,
                    omit_external=args.no_manifest,
                )
        else:
            parser.error(
                "Must specify which Runs to pack, --query ... or "
                "--uids ... or --all."
            )
        if not args.no_manfiest:
            print(f"Writing manifest of external files....")
            with another_manager.open(MANIFEST_FILE_NAME, "a") as file:
                file.write("\n".join(external_files))
        if failures:
            print(f"{len(failures)} Runs failed to pack.")
            with tempfile.NamedTemporaryFile("w", delete=False) as file:
                print(
                    "Writing unique IDs of Runs that failed to pack to " f"{file.name}"
                )
                file.write("\n".join(failures))
    finally:
        manager.close()
        another_manager.close()


class _ListCatalogsAction(argparse.Action):
    # a special action that allows the usage --list-catalogs to override
    # any 'required args' requirements, the same way that --help does

    def __init__(
        self,
        option_strings,
        dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS,
        help=None,
    ):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        import databroker

        for name in databroker.catalog:
            print(name)
        parser.exit()
