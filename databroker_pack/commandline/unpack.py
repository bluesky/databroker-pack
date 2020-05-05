#!/usr/bin/env python
import argparse
import sys
from .._unpack import unpack_inplace, unpack_mongo_normalized
from ._utils import ListCatalogsAction, ShowVersionAction
from .._utils import CatalogNameExists


def main():
    parser = argparse.ArgumentParser(
        description="Install a Catalog of packed Bluesky Runs.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.register("action", "show_version", ShowVersionAction)
    parser.register("action", "list_catalogs", ListCatalogsAction)
    parser.add_argument(
        "how",
        type=str,
        choices=("inplace", "mongo_normalized"),
        help="Read Documents in place (from files) or load them into a database.",
    )
    parser.add_argument("path", type=str, help="Path to pack directory")
    parser.add_argument("name", type=str, help="Name of new catalog")
    parser.add_argument("--no-merge", action="store_true")
    parser.add_argument(
        "--mongo-uri",
        type=str,
        help=(
            "MongoDB URI. Default is 'mongodb://localhost:27017/{database}' "
            "where the token {database}, if given, is filled in with "
            "'databroker_{name}. The value of {name} is taken from the name "
            "parameter above."
        ),
    )
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
    args = parser.parse_args()
    try:
        if args.how == "inplace":
            config_path = unpack_inplace(args.path, args.name, merge=not args.no_merge)
        elif args.how == "mongo_normalized":
            uri = args.mongo_uri or "mongodb://localhost:27017/{database}"
            formatted_uri = uri.format(database=f"databroker_{args.name}")
            config_path = unpack_mongo_normalized(
                args.path, formatted_uri, args.name, merge=not args.no_merge
            )
        # We rely on argparse to ensure that args.how is one of the above.
    except CatalogNameExists:
        import databroker
        import itertools
        import shutil

        term_size = shutil.get_terminal_size((100, 50))
        extant_catalogs = sorted(databroker.catalog)
        ncats = len(extant_catalogs)
        col_width = max(len(_) for _ in extant_catalogs) + 5
        ncols = max(term_size.columns // col_width, 1)

        format_str = f"{{:<{col_width}}}" * ncols

        n_rows = (ncats // ncols) + int(ncols % ncols > 0)

        cols = [extant_catalogs[j * n_rows: (j + 1) * n_rows] for j in range(ncols)]

        nice_names = "\n".join(
            format_str.format(*g) for g in itertools.zip_longest(*cols, fillvalue="")
        )

        msg = f"""
You tried to unpack to a catalog named {args.name} which already exists.

The currently existing catalogs on your system are:

{nice_names}

Please try unpacking again with a new name.
"""
        print(msg)
        sys.exit(1)
    else:
        print(f"Placed configuration file at {config_path!s}")


if __name__ == "__main__":
    main()
