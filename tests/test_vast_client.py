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
