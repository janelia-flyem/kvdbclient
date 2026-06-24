import os
import pathlib
import signal
import socket
import subprocess
import time
import uuid
from datetime import timedelta

import pytest

from kvdbclient.bigtable import BigTableConfig
from kvdbclient.bigtable.client import Client
from kvdbclient.hbase import HBaseConfig
from kvdbclient.hbase.client import Client as HBaseClient
from kvdbclient.vast.client import Client as VastClient
from hbase_mock_server import start_hbase_mock_server


EMULATOR_PROJECT = "test-project"
EMULATOR_INSTANCE = "test-instance"


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_port(host, port, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"Emulator on {host}:{port} not ready within {timeout}s")


# ── BigTable fixtures ────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def bigtable_emulator():
    """Start the BigTable emulator or use one already running (CI)."""
    existing = os.environ.get("BIGTABLE_EMULATOR_HOST")
    if existing:
        host, port = existing.rsplit(":", 1)
        _wait_for_port(host or "localhost", int(port))
        yield existing
        return

    port = _find_free_port()
    host_port = f"localhost:{port}"
    proc = subprocess.Popen(
        [
            "gcloud", "beta", "emulators", "bigtable", "start",
            f"--host-port={host_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    os.environ["BIGTABLE_EMULATOR_HOST"] = host_port
    _wait_for_port("localhost", port)
    yield host_port

    os.kill(proc.pid, signal.SIGTERM)
    proc.wait(timeout=10)
    os.environ.pop("BIGTABLE_EMULATOR_HOST", None)


@pytest.fixture(scope="session")
def bt_config(bigtable_emulator):
    return BigTableConfig(
        PROJECT=EMULATOR_PROJECT,
        INSTANCE=EMULATOR_INSTANCE,
        ADMIN=True,
        READ_ONLY=False,
        CREDENTIALS=None,
    )


@pytest.fixture()
def bt_client(bt_config):
    """Client with a fresh table already created."""
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = Client(table_id=table_id, config=bt_config)
    client.create_table(meta={"test": True}, version="0.0.1")
    yield client


@pytest.fixture()
def bt_client_no_table(bt_config):
    """Client bound to a table that does not yet exist."""
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = Client(table_id=table_id, config=bt_config)
    yield client


@pytest.fixture()
def bt_client_small_batch(bigtable_emulator):
    """Client with small MAX_ROW_KEY_COUNT to trigger sharded reads."""
    config = BigTableConfig(
        PROJECT=EMULATOR_PROJECT,
        INSTANCE=EMULATOR_INSTANCE,
        ADMIN=True,
        READ_ONLY=False,
        CREDENTIALS=None,
        MAX_ROW_KEY_COUNT=50,
    )
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = Client(table_id=table_id, config=config)
    client.create_table(meta={"test": True}, version="0.0.1")
    yield client


# ── HBase fixtures ───────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def hbase_server():
    _data, server, port = start_hbase_mock_server()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture(scope="session")
def hbase_config(hbase_server):
    return HBaseConfig(BASE_URL=hbase_server)


@pytest.fixture()
def hbase_client(hbase_config):
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = HBaseClient(table_id=table_id, config=hbase_config)
    client.create_table(meta={"test": True}, version="0.0.1")
    yield client


@pytest.fixture()
def hbase_client_no_table(hbase_config):
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = HBaseClient(table_id=table_id, config=hbase_config)
    yield client


@pytest.fixture()
def hbase_client_short_expiry(hbase_config):
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = HBaseClient(table_id=table_id, config=hbase_config, lock_expiry=timedelta(seconds=1))
    client.create_table(meta={"test": True}, version="0.0.1")
    yield client


@pytest.fixture()
def bt_client_short_expiry(bt_config):
    table_id = f"test_{uuid.uuid4().hex[:12]}"
    client = Client(table_id=table_id, config=bt_config, lock_expiry=timedelta(seconds=1))
    client.create_table(meta={"test": True}, version="0.0.1")
    yield client


# ── VAST live integration fixtures (gated, fail-closed) ──────────────────
#
# Seeded from the read-only probe tmp/inspect_vast.py in the zen-ACG workspace.
# These run ONLY when RUN_VAST_INTEGRATION=1 and a dedicated, non-production
# VAST_TEST_SCHEMA is set. They fail closed otherwise so a misconfigured run can
# never create/drop tables in the production `pcgvast` cells or the existing
# `autoproof` schema (pcgvast-0002 design decision D4). Helper/encoding tests
# stay non-gated; only tests that need a live backend take `vast_session`.

# Schemas integration tests must never create or drop tables in: the production
# PCG schema, the kvdbclient default, and the live neighbor observed on the
# Janelia cluster (sessions/reports/vast-live-grounding.md).
VAST_PROTECTED_SCHEMAS = frozenset({"pcgvast", "pychunkedgraph", "autoproof"})


def _load_dotenv(path):
    """Minimal ``KEY=VALUE`` .env reader (mirrors tmp/inspect_vast.py).

    Returns a dict; ignores comments/blank lines and strips one layer of quotes.
    Lets the gated suite read the same VAST_* credentials the kvdbclient.vast
    backend reads at runtime, without adding a python-dotenv dependency.
    """
    env = {}
    path = pathlib.Path(path)
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


@pytest.fixture(scope="session")
def vast_live_config():
    """Gated, fail-closed ``VastConfig`` pinned to a dedicated test schema.

    Skips unless ``RUN_VAST_INTEGRATION=1``. Loads ``<repo>/.env`` for the
    ``VAST_*`` connection vars (without clobbering vars already exported, so the
    pixi ``test-vast-live`` task's ``VAST_TEST_SCHEMA`` wins), then refuses to
    proceed unless a non-production ``VAST_TEST_SCHEMA`` and complete connection
    settings are present.
    """
    if os.environ.get("RUN_VAST_INTEGRATION") != "1":
        pytest.skip("live VAST integration disabled (set RUN_VAST_INTEGRATION=1)")

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    for key, value in _load_dotenv(repo_root / ".env").items():
        os.environ.setdefault(key, value)

    test_schema = os.environ.get("VAST_TEST_SCHEMA")
    if not test_schema:
        pytest.fail("VAST_TEST_SCHEMA must be set for live integration runs")
    if test_schema in VAST_PROTECTED_SCHEMAS:
        pytest.fail(
            f"refusing to run integration tests against protected schema "
            f"{test_schema!r}; use a dedicated test schema (e.g. pcgvast_test)"
        )

    from kvdbclient.vast import get_client_info

    config = get_client_info(schema=test_schema)
    missing = [
        name
        for name in ("ENDPOINT", "ACCESS_KEY", "SECRET_KEY", "BUCKET")
        if not getattr(config, name)
    ]
    if missing:
        pytest.fail(f"incomplete VAST_* connection settings: missing {missing}")
    assert config.SCHEMA == test_schema  # guard against env-precedence surprises
    return config


@pytest.fixture()
def vast_session(vast_live_config):
    """Open a live ``vastdb`` session from the gated config."""
    import vastdb

    session = vastdb.connect(
        endpoint=vast_live_config.ENDPOINT,
        access=vast_live_config.ACCESS_KEY,
        secret=vast_live_config.SECRET_KEY,
    )
    yield session


def vast_test_table_name():
    """A unique, fail-safe ``test_``-prefixed table name for live tests.

    pcgvast-0002 D4 requires integration tables to start with ``test_`` so a
    stray name can never collide with real PCG tables; teardown drops them.
    """
    return f"test_{uuid.uuid4().hex[:12]}"


def _drop_vast_test_table(config, table_id):
    if config.SCHEMA in VAST_PROTECTED_SCHEMAS:
        raise RuntimeError(f"refusing to drop from protected schema {config.SCHEMA!r}")
    if not table_id.startswith("test_"):
        raise RuntimeError(f"refusing to drop non-test VAST table {table_id!r}")

    import vastdb

    session = vastdb.connect(
        endpoint=config.ENDPOINT,
        access=config.ACCESS_KEY,
        secret=config.SECRET_KEY,
    )
    with session.transaction() as tx:
        schema = tx.bucket(config.BUCKET).schema(config.SCHEMA, fail_if_missing=False)
        if schema is None:
            return
        table = schema.table(table_id, fail_if_missing=False)
        if table is not None:
            table.drop()


@pytest.fixture()
def vast_client(vast_live_config):
    table_id = vast_test_table_name()
    client = VastClient(table_id=table_id, config=vast_live_config)
    created = False
    try:
        client.create_table(meta={"test": True}, version="0.0.1")
        created = True
        yield client
    finally:
        client.close()
        if created:
            _drop_vast_test_table(vast_live_config, table_id)


@pytest.fixture()
def vast_client_no_table(vast_live_config):
    table_id = vast_test_table_name()
    client = VastClient(table_id=table_id, config=vast_live_config)
    try:
        yield client
    finally:
        client.close()
        _drop_vast_test_table(vast_live_config, table_id)


@pytest.fixture()
def vast_client_small_batch(vast_live_config):
    config = vast_live_config._replace(MAX_ROW_KEY_COUNT=2)
    table_id = vast_test_table_name()
    client = VastClient(table_id=table_id, config=config)
    created = False
    try:
        client.create_table(meta={"test": True}, version="0.0.1")
        created = True
        yield client
    finally:
        client.close()
        if created:
            _drop_vast_test_table(config, table_id)
