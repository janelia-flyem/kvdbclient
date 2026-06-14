"""
Sub packages/modules for backend storage clients.
Supports Google BigTable and Apache HBase.

A simple client needs to be able to create the table,
store table meta and to write and read node information.
Also needs locking support to prevent race conditions
when modifying root/parent nodes.

In addition, clients with more features like generating unique IDs
and logging facilities can be implemented by inherting respective base classes.

These methods are in separate classes because they are logically related.
This also makes it possible to have different backend storage solutions,
making it possible to use any unique features these solutions may provide.

Please see `base.py` for more details.
"""

import warnings
from collections import namedtuple
from os import environ
from typing import Union

warnings.filterwarnings("ignore", message="urllib3", module="requests")

from .base import ColumnFamilyConfig, DEFAULT_COLUMN_FAMILIES
from .bigtable import BigTableConfig
from .bigtable import get_client_info as get_bigtable_client_info
from .bigtable.client import Client as BigTableClient
from .hbase import HBaseConfig
from .hbase import get_client_info as get_hbase_client_info
from .hbase.client import Client as HBaseClient
from .vast import VastConfig
from .vast import get_client_info as get_vast_client_info
from .vast.client import Client as VastClient

ClientType = Union[BigTableClient, HBaseClient, VastClient]


_backend_clientinfo_fields = ("TYPE", "CONFIG")
_backend_clientinfo_defaults = ("bigtable", None)
BackendClientInfo = namedtuple(
    "BackendClientInfo",
    _backend_clientinfo_fields,
    defaults=_backend_clientinfo_defaults,
)


def get_client_class(backend_type: str = "bigtable"):
    """Return the client class for the given backend type."""
    backend_type = (backend_type or "bigtable").lower()
    if backend_type == "bigtable":
        return BigTableClient
    elif backend_type == "hbase":
        return HBaseClient
    elif backend_type == "vast":
        return VastClient
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")


def get_default_client_info():
    """
    Load client from env variables.
    """
    backend_type = environ.get("PCG_BACKEND_TYPE", "bigtable").lower()
    if backend_type == "hbase":
        return BackendClientInfo(
            TYPE="hbase", CONFIG=get_hbase_client_info()
        )
    if backend_type == "vast":
        return BackendClientInfo(
            TYPE="vast", CONFIG=get_vast_client_info()
        )

    return BackendClientInfo(
        TYPE="bigtable", CONFIG=get_bigtable_client_info(admin=True, read_only=False)
    )
