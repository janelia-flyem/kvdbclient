from collections import namedtuple
from os import environ

DEFAULT_ENDPOINT = "http://localhost:9090"
DEFAULT_SCHEMA = "pychunkedgraph"

_vastconfig_fields = (
    "ENDPOINT",
    "ACCESS_KEY",
    "SECRET_KEY",
    "BUCKET",
    "SCHEMA",
    "MAX_ROW_KEY_COUNT",
)
_vastconfig_defaults = (
    environ.get("VAST_ENDPOINT", DEFAULT_ENDPOINT),
    environ.get("VAST_ACCESS_KEY"),
    environ.get("VAST_SECRET_KEY"),
    environ.get("VAST_BUCKET"),
    environ.get("VAST_SCHEMA", DEFAULT_SCHEMA),
    1000,
)
VastConfig = namedtuple(
    "VastConfig", _vastconfig_fields, defaults=_vastconfig_defaults
)


def get_client_info(
    endpoint: str = None,
    bucket: str = None,
    schema: str = None,
):
    """Helper function to load config from env."""
    _endpoint = environ.get("VAST_ENDPOINT", DEFAULT_ENDPOINT)
    if endpoint:
        _endpoint = endpoint

    _bucket = environ.get("VAST_BUCKET")
    if bucket:
        _bucket = bucket

    _schema = environ.get("VAST_SCHEMA", DEFAULT_SCHEMA)
    if schema:
        _schema = schema

    return VastConfig(
        ENDPOINT=_endpoint,
        ACCESS_KEY=environ.get("VAST_ACCESS_KEY"),
        SECRET_KEY=environ.get("VAST_SECRET_KEY"),
        BUCKET=_bucket,
        SCHEMA=_schema,
    )
