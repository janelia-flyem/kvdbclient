# pylint: disable=missing-docstring, redefined-outer-name

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

import kvdbclient
from kvdbclient import attributes
from kvdbclient import basetypes
from kvdbclient.base import ColumnFamilyConfig
from kvdbclient.serializers import serialize_uint64
from kvdbclient.vast import VastConfig
from kvdbclient.vast import utils as vast_utils
from kvdbclient.vast.client import Client as VastClient


def write_node(client, node_id, val_dict, time_stamp=None):
    row_key = serialize_uint64(np.uint64(node_id))
    entry = client.mutate_row(row_key, val_dict, time_stamp=time_stamp)
    client.write([entry])
    return row_key


# -- Wiring and pure helpers ------------------------------------------------------


class TestVastFactoryWiring:
    def test_factory_resolves_vast(self):
        assert kvdbclient.get_client_class("vast") is VastClient

    def test_default_client_info_honors_env(self, monkeypatch):
        monkeypatch.setenv("PCG_BACKEND_TYPE", "vast")
        info = kvdbclient.get_default_client_info()
        assert info.TYPE == "vast"
        assert isinstance(info.CONFIG, VastConfig)

    def test_client_is_concrete(self):
        assert VastClient.__abstractmethods__ == frozenset()


class TestVastConfigRedaction:
    def test_repr_and_str_redact_secrets(self):
        config = VastConfig(
            ENDPOINT="http://vast.example",
            ACCESS_KEY="ACCESS-SECRET-1234",
            SECRET_KEY="SECRET-SECRET-5678",
            BUCKET="bucket",
            SCHEMA="pcgvast_test",
        )
        text = repr(config)
        assert "ACCESS-SECRET-1234" not in text
        assert "SECRET-SECRET-5678" not in text
        assert "***1234" in text
        assert "***5678" in text
        assert str(config) == text

    def test_replace_preserves_type_and_redaction(self):
        config = VastConfig(ACCESS_KEY="abcd1234", SECRET_KEY="efgh5678")
        replaced = config._replace(SCHEMA="pcgvast_test")
        assert isinstance(replaced, VastConfig)
        assert "abcd1234" not in repr(replaced)
        assert replaced.SCHEMA == "pcgvast_test"


class TestVastHelpers:
    def test_row_key_codec_is_strict_utf8(self):
        for row_key in [
            b"00000000000000000123",
            b"f00000000000000000123",
            b"i00000000000000000123_7",
            b"meta",
            b"version",
            b"ioperations",
            b"params",
        ]:
            encoded = vast_utils.encode_row_key(row_key)
            assert vast_utils.decode_row_key(encoded) == row_key

        with pytest.raises(UnicodeDecodeError):
            vast_utils.encode_row_key(b"\xff")

    def test_timestamp_rounding_and_utc_conversion(self):
        ts = datetime(2026, 6, 22, 12, 0, 0, 123456, tzinfo=timezone.utc)
        assert vast_utils.get_vast_compatible_time_stamp(ts).microsecond == 123000
        assert (
            vast_utils.get_vast_compatible_time_stamp(ts, round_up=True).microsecond
            == 124000
        )

        exact = datetime(2026, 6, 22, 12, 0, 0, 123000, tzinfo=timezone.utc)
        assert vast_utils.get_vast_compatible_time_stamp(exact) == exact
        assert vast_utils.get_vast_compatible_time_stamp(
            exact, round_up=True
        ) == exact + timedelta(milliseconds=1)

    def test_cells_schema(self):
        schema = vast_utils.cells_schema()
        assert schema.names == ["row_key", "family", "qualifier", "ts", "value"]
        assert [field.name for field in schema] == list(vast_utils.CELL_FIELD_NAMES)

    def test_attribute_mapping(self):
        attr = attributes.Hierarchy.Child
        family, qualifier = vast_utils.attribute_to_columns(attr)
        assert vast_utils.attribute_from_columns(family, qualifier) is attr

    def test_grouping_deserializes_newest_first(self):
        old_ts = datetime(2026, 6, 22, 12, 0, 0, 0, tzinfo=timezone.utc)
        new_ts = old_ts + timedelta(milliseconds=1)
        attr = attributes.TableMeta.Meta
        rows = [
            {
                vast_utils.ROW_KEY: "meta",
                vast_utils.FAMILY: attr.family_id,
                vast_utils.QUALIFIER: attr.key,
                vast_utils.TIMESTAMP: vast_utils.to_vast_timestamp(old_ts),
                vast_utils.VALUE: attr.serialize({"version": 1}),
            },
            {
                vast_utils.ROW_KEY: "meta",
                vast_utils.FAMILY: attr.family_id,
                vast_utils.QUALIFIER: attr.key,
                vast_utils.TIMESTAMP: vast_utils.to_vast_timestamp(new_ts),
                vast_utils.VALUE: attr.serialize({"version": 2}),
            },
        ]
        grouped = vast_utils.rows_to_column_dicts(rows)
        cells = grouped[b"meta"][attr]
        assert [cell.value for cell in cells] == [{"version": 2}, {"version": 1}]
        assert all(cell.timestamp.tzinfo is timezone.utc for cell in cells)

    def test_chunked(self):
        assert list(vast_utils.chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]


# -- Live connectivity and read/write primitives ---------------------------------


@pytest.mark.integration
class TestVastLiveConnectivity:
    def test_session_and_elysium_gate(self, vast_session):
        vast_session.features.check_elysium()
        assert vast_session.features.vast_version >= (5, 3)

    def test_lists_schemas_readonly(self, vast_session, vast_live_config):
        with vast_session.transaction() as tx:
            bucket = tx.bucket(vast_live_config.BUCKET)
            schema_names = [s.name for s in bucket.schemas()]
        assert isinstance(schema_names, list)


@pytest.mark.integration
class TestVastCreateTable:
    def test_creates_table_and_sets_meta(self, vast_client_no_table):
        client = vast_client_no_table
        meta = {"chunk_size": 512}
        client.create_table(meta, "1.0")
        assert client.read_table_version() == "1.0"
        assert client.read_table_meta() == meta

    def test_raises_on_duplicate_but_allows_validated_reuse(self, vast_client):
        with pytest.raises(ValueError, match="already exists"):
            vast_client.create_table({}, "2.0")
        vast_client.create_table({}, "2.0", fail_if_exists=False)
        assert vast_client.read_table_version() == "0.0.1"

    def test_custom_column_families(self, vast_client_no_table):
        families = [
            ColumnFamilyConfig("0"),
            ColumnFamilyConfig("1", max_versions=3),
            ColumnFamilyConfig("2", max_age=timedelta(days=30)),
        ]
        client = vast_client_no_table
        client.create_table({"custom": True}, "1.0", column_families=families)
        assert client.read_table_version() == "1.0"
        assert client.read_table_meta() == {"custom": True}


@pytest.mark.integration
class TestVastColumnFamily:
    def test_create_extra_family_records_metadata(self, vast_client):
        vast_client.create_column_family(ColumnFamilyConfig("6", max_versions=2))
        meta = vast_client.read_table_meta()
        assert meta["_vast_column_families"]["6"]["max_versions"] == 2


@pytest.mark.integration
class TestVastTableMeta:
    def test_read_table_meta(self, vast_client):
        assert vast_client.read_table_meta() == {"test": True}

    def test_update_table_meta(self, vast_client):
        vast_client.update_table_meta({"new_field": 42})
        assert vast_client.read_table_meta() == {"new_field": 42}

    def test_update_table_meta_overwrite(self, vast_client):
        vast_client.update_table_meta({"replaced": True}, overwrite=True)
        assert vast_client.read_table_meta() == {"replaced": True}

    def test_read_table_version(self, vast_client):
        assert vast_client.read_table_version() == "0.0.1"

    def test_add_table_version_overwrite(self, vast_client):
        vast_client.add_table_version("2.0", overwrite=True)
        assert vast_client.read_table_version() == "2.0"

    def test_add_table_version_duplicate_raises(self, vast_client):
        with pytest.raises(AssertionError):
            vast_client.add_table_version("1.0", overwrite=False)


@pytest.mark.integration
class TestVastReadWrite:
    def test_single_row(self, vast_client):
        arr = np.array([10, 20, 30], dtype=basetypes.NODE_ID)
        write_node(vast_client, 100, {attributes.Hierarchy.Child: arr})
        data = vast_client.read_node(np.uint64(100), properties=attributes.Hierarchy.Child)
        np.testing.assert_array_equal(data[0].value, arr)

    def test_multiple_properties(self, vast_client):
        child_arr = np.array([10], dtype=basetypes.NODE_ID)
        parent_val = np.uint64(42)
        write_node(
            vast_client,
            100,
            {
                attributes.Hierarchy.Child: child_arr,
                attributes.Hierarchy.Parent: parent_val,
            },
        )
        data = vast_client.read_node(
            np.uint64(100),
            properties=[attributes.Hierarchy.Child, attributes.Hierarchy.Parent],
        )
        assert attributes.Hierarchy.Child in data
        assert attributes.Hierarchy.Parent in data

    def test_bulk_write(self, vast_client):
        entries = []
        for i in range(5):
            node_id = np.uint64(200 + i)
            arr = np.array([i], dtype=basetypes.NODE_ID)
            entries.append(
                vast_client.mutate_row(
                    serialize_uint64(node_id),
                    {attributes.Hierarchy.Child: arr},
                )
            )
        vast_client.write(entries)
        for i in range(5):
            data = vast_client.read_node(
                np.uint64(200 + i), properties=attributes.Hierarchy.Child
            )
            np.testing.assert_array_equal(
                data[0].value, np.array([i], dtype=basetypes.NODE_ID)
            )

    def test_empty_write_noop(self, vast_client):
        vast_client.write([])

    def test_point_batch_and_range_reads(self, vast_client_small_batch):
        for nid in [100, 200, 300]:
            write_node(
                vast_client_small_batch,
                nid,
                {attributes.Hierarchy.Child: np.array([nid], dtype=basetypes.NODE_ID)},
            )

        by_ids = vast_client_small_batch.read_nodes(
            node_ids=[np.uint64(100), np.uint64(200), np.uint64(300)],
            properties=attributes.Hierarchy.Child,
        )
        assert len(by_ids) == 3

        by_range = vast_client_small_batch.read_nodes(
            start_id=np.uint64(100),
            end_id=np.uint64(301),
            properties=attributes.Hierarchy.Child,
        )
        assert len(by_range) == 3

    def test_time_travel_returns_newest_at_or_before(self, vast_client):
        attr = attributes.Hierarchy.Parent
        row_key = serialize_uint64(np.uint64(100))
        t0 = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(milliseconds=1)
        vast_client.write([vast_client.mutate_row(row_key, {attr: np.uint64(1)}, t0)])
        vast_client.write([vast_client.mutate_row(row_key, {attr: np.uint64(2)}, t1)])

        inclusive = vast_client.read_node(
            np.uint64(100),
            properties=attr,
            end_time=t0,
            end_time_inclusive=True,
        )
        assert [cell.value for cell in inclusive] == [np.uint64(1)]

        exclusive = vast_client.read_node(
            np.uint64(100),
            properties=attr,
            end_time=t0,
            end_time_inclusive=False,
        )
        assert exclusive == []

        newest = vast_client.read_node(np.uint64(100), properties=attr)
        assert [cell.value for cell in newest] == [np.uint64(2), np.uint64(1)]

    def test_read_all_rows(self, vast_client):
        write_node(
            vast_client,
            100,
            {attributes.Hierarchy.Child: np.array([100], dtype=basetypes.NODE_ID)},
        )
        result = vast_client.read_all_rows()
        assert len(result.rows) >= 3
        assert b"meta" in result.rows
        result.consume_all()


@pytest.mark.integration
class TestVastDelete:
    def test_delete_row(self, vast_client):
        row_key = write_node(
            vast_client,
            100,
            {attributes.Hierarchy.Child: np.array([10], dtype=basetypes.NODE_ID)},
        )
        assert vast_client.read_node(np.uint64(100))
        vast_client.delete_row(row_key)
        assert vast_client.read_node(np.uint64(100)) == {}

    def test_delete_row_nonexistent_is_noop(self, vast_client):
        vast_client.delete_row(b"nonexistent")

    def test_delete_cells_with_row_keys(self, vast_client):
        key1 = write_node(
            vast_client,
            100,
            {attributes.Hierarchy.Child: np.array([10], dtype=basetypes.NODE_ID)},
        )
        write_node(
            vast_client,
            200,
            {attributes.Hierarchy.Child: np.array([20], dtype=basetypes.NODE_ID)},
        )
        vast_client.delete_cells([], row_keys_to_delete=[key1])
        assert vast_client.read_node(np.uint64(100)) == {}
        assert vast_client.read_node(np.uint64(200))

    def test_delete_specific_cell_version(self, vast_client):
        attr = attributes.Hierarchy.Parent
        row_key = serialize_uint64(np.uint64(100))
        t0 = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(milliseconds=1)
        vast_client.write([vast_client.mutate_row(row_key, {attr: np.uint64(1)}, t0)])
        vast_client.write([vast_client.mutate_row(row_key, {attr: np.uint64(2)}, t1)])

        vast_client.delete_cells([(row_key, attr, [t1])])
        remaining = vast_client.read_node(np.uint64(100), properties=attr)
        assert [cell.value for cell in remaining] == [np.uint64(1)]

    def test_delete_meta(self, vast_client):
        vast_client._delete_meta()
        assert vast_client.read_table_meta() is None


_DEFERRED = "VAST locks/ID generation deferred to LockProvider/Sequencer slice"


@pytest.mark.skip(reason=_DEFERRED)
class TestVastLocks:
    def test_lock_root_is_atomic(self):
        raise NotImplementedError(_DEFERRED)

    def test_row_key_indefinite_lock(self):
        raise NotImplementedError(_DEFERRED)


@pytest.mark.skip(reason=_DEFERRED)
class TestVastIdGen:
    def test_get_ids_range_is_monotonic_and_nonoverlapping(self):
        raise NotImplementedError(_DEFERRED)
