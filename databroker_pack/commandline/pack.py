#!/usr/bin/env python
import argparse
import functools
import logging
import os
import pathlib
import sys
import tempfile

from suitcase.utils import MultiFileManager

from ._utils import ListCatalogsAction, ShowVersionAction
from .._pack import (
    copy_external_files,
    export_catalog,
    export_uids,
    write_documents_manifest,
    write_jsonl_catalog_file,
    write_msgpack_catalog_file,
    write_external_files_manifest,
)


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
    # By making a group "other" we can place the less-frequently-used options
    # at the bottom.
    other_group = parser.add_argument_group()
    parser.add_argument("catalog", type=str, help="Catalog name")
    parser.add_argument("directory", type=str, help="Path to output directory")
    parser.add_argument(
        "--list-catalogs",
        action="list_catalogs",
        default=argparse.SUPPRESS,
        help="List allowed values for catalog and exit.",
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
        "--ignore-external",
        action="store_true",
        help=(
            "By default, the locations of all relevant external files on the "
            "source machine are written to text files. "
            "Set this to omit those manifests."
        ),
    )
    other_group.add_argument(
        "--format",
        default="msgpack",
        choices=["msgpack", "jsonl"],
        type=str.lower,  # Makes choices case-insensitive
        help=(
            "Format for Documents in the pack output. Choise msgpack "
            "(default) for binary speed/compactness. Choose jsonl for "
            "plaintext."
        ),
    )
    other_group.add_argument(
        "--no-documents", action="store_true", help="Do not pack the Documents.",
    )
    other_group.add_argument(
        "--limit",
        type=int,
        help=(
            "Stop after exporting some number of Runs. Useful for testing a "
            "subset before doing a lengthy export."
        ),
    )
    other_group.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Exit when error occurs. Otherwise failures are logged to "
            "stderr as they happen "
            "and again all together at the end."
        ),
    )
    other_group.add_argument(
        "--handler-registry",
        help=(
            "Dict mapping specs to handler objects like "
            "\"{'AD_HDF5': 'area_detector_handlers.handlers:AreaDetectorHDF5Handler'}\" "
            "If unspecified, automatic handler discovery is used."
        ),
    )
    other_group.add_argument(
        "--salt",
        type=str.encode,  # casts input to bytes
        help=("Set this to override the random default with a fixed value."),
    )
    other_group.add_argument(
        "-V",
        "--version",
        action="show_version",
        default=argparse.SUPPRESS,
        help="Show databroker_pack version and exit.",
    )
    args = parser.parse_args()
    # We hide the imports here just for speed.
    if args.format == "msgpack":
        import suitcase.msgpack

        serializer_class = suitcase.msgpack.Serializer
    if args.format == "jsonl":
        import suitcase.jsonl

        serializer_class = suitcase.jsonl.Serializer
    if args.ignore_external:
        external = "ignore"
    elif args.fill_external:
        external = "fill"
    else:
        external = None
    import databroker
    import databroker.queries
    import databroker.core

    if args.handler_registry is not None:
        # Temporary limitation due to the fact that BlueskyRun.get_file_list()
        # does not accept a custom Filler.
        if not args.fill_external:
            raise NotImplementedError(
                "The --handler-registry parameter currently only works with "
                "--fill-external. All other modes of handling external data "
                "use automatic handler discovery only."
            )
        # There are two kinds of "parsing" happening here.
        # 1. Go from stdin string to dict using ast.literal_eval.
        # 2. Go from dict of strings to dict of actual handler classes using
        #    parse_handler_registry.
        try:
            import ast

            handler_registry = ast.literal_eval(args.handler_registry)
        except Exception as exc:
            raise ValueError(
                f"Could not parse --handler-registry {args.handler_registry}. "
                "A dict of strings is expected."
            ) from exc
        handler_registry = databroker.core.parse_handler_registry(handler_registry)
    else:
        handler_registry = None
    # The MultiFileManager would create this directory for us on demand (when
    # we open the first file), but let's create it early so we can check
    # permissions.
    try:
        os.makedirs(args.directory, exist_ok=True)
    except Exception as exc:
        print(f"Could not create directory at {args.directory}.")
        print(f"Error: {exc!r}")
        sys.exit(1)
    if not os.access(args.directory, os.W_OK):
        print(f"Directory at {args.directory} is not writable.")
        sys.exit(1)
    catalog = databroker.catalog[args.catalog]()
    manager = MultiFileManager(args.directory)
    try:
        # We write the catalog file at the end because we need to collect
        # information to write the root_map. We never overwrite existing files,
        # so we will fail if catalog.yml exists already. In order to save time
        # and fail early, check that the file exists.
        catalog_file_path = pathlib.Path(args.directory, "catalog.yml")
        if os.path.isfile(catalog_file_path):
            print(
                f"The file {catalog_file_path} exists. Specify an empty directory, or "
                "a nonexistent one (which will be created by "
                f"{os.path.basename(sys.argv[0])})."
            )
            sys.exit(1)
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
                    raise ValueError(f"Could not parse query {raw_query}.")
                queries.append(query)
            combined_query = {"$and": queries}
            # HACK We need no_cursor_timeout only until databroker learns to
            # seamlessly remake cursors when they time out.
            try:
                results = catalog.search(combined_query, no_cursor_timeout=True)
            except TypeError:
                # Drivers that are not Mongo drivers will not support
                # no_cursor_timeout.
                results = catalog.search(combined_query)
            if not results:
                print(f"Query {combined_query} yielded no results. Exiting.")
                sys.exit(1)
            artifacts, external_files, failures = export_catalog(
                results,
                manager,
                strict=args.strict,
                external=external,
                no_documents=args.no_documents,
                handler_registry=handler_registry,
                serializer_class=serializer_class,
                salt=args.salt,
                limit=args.limit,
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
            if not uids:
                print("Found empty input for --uids. Exiting")
                sys.exit(1)
            artifacts, external_files, failures = export_uids(
                catalog,
                uids[:args.limit],
                manager,
                strict=args.strict,
                external=external,
                no_documents=args.no_documents,
                handler_registry=handler_registry,
                serializer_class=serializer_class,
                salt=args.salt,
            )
        else:
            parser.error(
                "Must specify which Runs to pack, --query ... or "
                "--uids ... or --all."
            )
        if not args.no_documents and artifacts.get("all"):
            write_documents_manifest(manager, args.directory, artifacts["all"])
        root_map = {}
        copying_failures = []
        if external is None:
            # When external is None, external data is neither being filled into
            # the Documents (external == 'fill') nor ignored (external ==
            # 'ignore') so we have to provide a root_map in the catalog file to
            # reference its location.
            if args.copy_external:
                target_drectory = pathlib.Path(args.directory, "external_files")
                for (
                    (root_in_document, root, unique_id),
                    files,
                ) in external_files.items():
                    new_root, new_files, copying_failures_ = copy_external_files(
                        target_drectory, root, unique_id, files, strict=args.strict
                    )
                    copying_failures.extend(copying_failures_)
                    # The root_map value will be the relative path to
                    # the data within the pack directory.
                    relative_root = new_root.relative_to(args.directory)
                    if not args.no_documents:
                        # When we are exporting documents, we rewrite the
                        # 'root' key in the Resource to unique_id to ensure
                        # no collisions of the keys in root_map.
                        root_map.update({unique_id: relative_root})
                    else:
                        # If we are not exporting documents, the root_map has
                        # to refer to the root as it is.
                        root_map.update({root_in_document: relative_root})
                    rel_paths = [
                        pathlib.Path(f).relative_to(args.directory) for f in new_files
                    ]
                    write_external_files_manifest(manager, unique_id, rel_paths)
            else:
                for (
                    (root_in_document, root, unique_id),
                    files,
                ) in external_files.items():
                    # The root_map value will be the current absolute path to
                    # the data.
                    if not args.no_documents:
                        # When we are exporting documents, we rewrite the
                        # 'root' key in the Resource to unique_id to ensure
                        # no collisions of the keys in root_map.
                        root_map.update({unique_id: root})
                    else:
                        # If we are not exporting documents, the root_map has
                        # to refer to the root as it is.
                        root_map.update({root_in_document: root})
                    write_external_files_manifest(manager, unique_id, files)
        if args.format == "jsonl":
            paths = ["./documents/*.jsonl"]
            write_jsonl_catalog_file(manager, args.directory, paths, root_map)
        elif args.format == "msgpack":
            paths = ["./documents/*.msgpack"]
            write_msgpack_catalog_file(manager, args.directory, paths, root_map)
        # No need for an else here; we validated that it is one of these above.
        if failures:
            print(f"{len(failures)} Runs failed to pack.")
            with tempfile.NamedTemporaryFile("w", delete=False) as file:
                print(f"See {file.name} for a list of uids of Runs that failed.")
                file.write("\n".join(failures))
        if copying_failures:
            with tempfile.NamedTemporaryFile("w", delete=False) as file:
                print(f"See {file.name} for a list of files that failed to copy.")
                file.write("\n".join(copying_failures))
        if failures or copying_failures:
            print(f"See {error_logfile_name} for error logs with more information.")
            sys.exit(1)
    finally:
        manager.close()


if __name__ == "__main__":
    main()
