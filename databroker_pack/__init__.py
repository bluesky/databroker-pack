from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from ._pack import *  # noqa
from ._unpack import *  # noqa
