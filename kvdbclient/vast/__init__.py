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
    "SORTED",
)


def _env_bool(name, default):
    value = environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


_vastconfig_defaults = (
    environ.get("VAST_ENDPOINT", DEFAULT_ENDPOINT),
    environ.get("VAST_ACCESS_KEY"),
    environ.get("VAST_SECRET_KEY"),
    environ.get("VAST_BUCKET"),
    environ.get("VAST_SCHEMA", DEFAULT_SCHEMA),
    1000,
    _env_bool("VAST_SORTED", True),
)
_VastConfigBase = namedtuple(
    "VastConfig", _vastconfig_fields, defaults=_vastconfig_defaults
)


def _redact_secret(value):
    if value is None:
        return None
    value = str(value)
    if len(value) <= 4:
        return "***"
    return f"***{value[-4:]}"


class VastConfig(_VastConfigBase):
    __slots__ = ()

    def __repr__(self):
        values = []
        for name, value in zip(self._fields, self):
            if name in {"ACCESS_KEY", "SECRET_KEY"}:
                value = _redact_secret(value)
            values.append(f"{name}={value!r}")
        return f"{type(self).__name__}({', '.join(values)})"

    __str__ = __repr__


def get_client_info(
    endpoint: str = None,
    bucket: str = None,
    schema: str = None,
    sorted: bool = None,
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

    _sorted = _env_bool("VAST_SORTED", True)
    if sorted is not None:
        _sorted = sorted

    return VastConfig(
        ENDPOINT=_endpoint,
        ACCESS_KEY=environ.get("VAST_ACCESS_KEY"),
        SECRET_KEY=environ.get("VAST_SECRET_KEY"),
        BUCKET=_bucket,
        SCHEMA=_schema,
        SORTED=_sorted,
    )
