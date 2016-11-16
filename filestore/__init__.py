from __future__ import absolute_import

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

try:
    from .fs import FileStore, FileStoreRO
except ImportError:
    pass

from .core import DatumNotFound

from .handlers_base import HandlerBase
from .path_only_handlers import RawHandler
