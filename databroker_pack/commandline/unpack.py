#!/usr/bin/env python
import argparse
import sys
from .._unpack import unpack
from ._utils import ListCatalogsAction, ShowVersionAction
from .._utils import CatalogNameExists

def main():
    parser = argparse.ArgumentParser(
        description="Install a Catalog of packed Bluesky Runs.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.register("action", "show_version", ShowVersionAction)
    parser.register("action", "list_catalogs", ListCatalogsAction)
    parser.add_argument("path", type=str, help="Path to pack directory")
    parser.add_argument("name", type=str, help="Name of new catalog")
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
        config_path = unpack(args.path, args.name)
    except CatalogNameExists:
        import databroker
        import itertools
        import shutil
        term_size = shutil.get_terminal_size((100, 50))
        extant_catalogs = sorted(databroker.catalog)
        col_width = max(len(_) for _ in extant_catalogs) + 5
        ncols = max(term_size.columns // col_width, 1)

        format_str = f'{{:<{col_width}}}'*ncols

        def grouper(iterable, n, fillvalue=''):
            "Collect data into fixed-length chunks or blocks"
            # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
            args = [iter(iterable)] * n
            return itertools.zip_longest(*args, fillvalue=fillvalue)

        nice_names = "\n".join(format_str.format(*g) for g in grouper(extant_catalogs, ncols))

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
