import argparse
from .._version import get_versions


class ShowVersionAction(argparse.Action):
    # a special action that allows the usage --version to override
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
        print(get_versions()["version"])
        parser.exit()


class ListCatalogsAction(argparse.Action):
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
