#!/usr/bin/env python
import argparse

from .._unpack import unpack
from ._utils import ListCatalogsAction, ShowVersionAction


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
    config_path = unpack(args.path, args.name)
    print(f"Placed configuration file at {config_path!s}")


if __name__ == "__main__":
    main()
