# pylint: disable=missing-docstring, redefined-outer-name

import pytest

import kvdbclient
from kvdbclient.vast import VastConfig
from kvdbclient.vast.client import Client as VastClient


# ── Wiring (no live backend needed) ──────────────────────────────────────


class TestVastFactoryWiring:
    def test_factory_resolves_vast(self):
        assert kvdbclient.get_client_class("vast") is VastClient

    def test_default_client_info_honors_env(self, monkeypatch):
        monkeypatch.setenv("PCG_BACKEND_TYPE", "vast")
        info = kvdbclient.get_default_client_info()
        assert info.TYPE == "vast"
        assert isinstance(info.CONFIG, VastConfig)

    def test_client_is_concrete(self):
        # Every base @abstractmethod must be implemented for PCG to instantiate
        # the backend; an empty set means the scaffold covers the full contract.
        assert VastClient.__abstractmethods__ == frozenset()


# ── Live connectivity (gated; seed of the pcgvast-0002 live path) ─────────
#
# Proves the read/connect path against the real Janelia VAST-DB that the probe
# tmp/inspect_vast.py demonstrated, now as a gated test. Runs only when
# RUN_VAST_INTEGRATION=1 with a fail-closed VAST_TEST_SCHEMA (see conftest's
# vast_session / vast_live_config). Makes no writes. The read/write/meta cases
# below remain skipped until the pcgvast-0002 primitives land.


@pytest.mark.integration
class TestVastLiveConnectivity:
    def test_session_and_elysium_gate(self, vast_session):
        # pcgvast-0002 D5: sorted (Elysium) tables are a runtime hard gate.
        # check_elysium() raises NotSupportedVersion on an unsupported cluster.
        vast_session.features.check_elysium()
        assert vast_session.features.vast_version >= (5, 3)

    def test_lists_schemas_readonly(self, vast_session, vast_live_config):
        # A real read against the bucket; confirms creds + endpoint resolve.
        with vast_session.transaction() as tx:
            bucket = tx.bucket(vast_live_config.BUCKET)
            schema_names = [s.name for s in bucket.schemas()]
        assert isinstance(schema_names, list)


# ── Use-case parity with bigtable/hbase (pending VAST primitives) ─────────
#
# As each primitive lands, port the corresponding cases from
# tests/test_bigtable_client.py and tests/test_hbase_client.py here so the VAST
# client matches every use-case pattern PCG exercises (Akhilesh Halageri's
# recommendation). Skipped until the primitives are implemented.

_PENDING = "VAST primitives pending (pcgvast-0001)"


@pytest.mark.skip(reason=_PENDING)
class TestVastCreateTable:
    def test_creates_table_and_sets_meta(self):
        raise NotImplementedError(_PENDING)

    def test_custom_column_families(self):
        raise NotImplementedError(_PENDING)


@pytest.mark.skip(reason=_PENDING)
class TestVastReadWrite:
    def test_round_trips_a_node(self):
        raise NotImplementedError(_PENDING)

    def test_point_and_batch_and_range_reads(self):
        raise NotImplementedError(_PENDING)

    def test_time_travel_returns_newest_at_or_before(self):
        raise NotImplementedError(_PENDING)


@pytest.mark.skip(reason=_PENDING)
class TestVastLocks:
    def test_lock_root_is_atomic(self):
        raise NotImplementedError(_PENDING)

    def test_row_key_indefinite_lock(self):
        raise NotImplementedError(_PENDING)


@pytest.mark.skip(reason=_PENDING)
class TestVastIdGen:
    def test_get_ids_range_is_monotonic_and_nonoverlapping(self):
        raise NotImplementedError(_PENDING)
