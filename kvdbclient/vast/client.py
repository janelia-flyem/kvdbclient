"""
VAST / VAST-DB backend client for kvdbclient.

Scaffold only (pcgvast-0001): this class mirrors the BigTable and HBase clients
and satisfies the full ``kvdbclient.base`` contract so the factory can resolve
it and PCG can (eventually) instantiate it, but the storage / lock / ID-range
primitives are not yet implemented. Design lives in the zen-ACG workspace:
``sessions/changes/pcgvast-0001-vast-backend-scaffold-and-tests.md`` and
``sessions/brainstorming/2026-06-14-pcg-v3-vast-keyvalue-backend.md``.

The ``vastdb`` import is intentionally lazy (see ``_connect``) so that
``import kvdbclient`` works without the optional ``vast`` extra installed.
"""

import typing
from datetime import timedelta

from . import VastConfig
from ..base import ClientWithIDGen
from ..base import OperationLogger

_PENDING = "VAST backend primitive not yet implemented — pcgvast-0001"


class Client(ClientWithIDGen, OperationLogger):
    """Key-value backend over VAST-DB (+ VAST S3 for bulk immutable blobs).

    Mirrors ``kvdbclient.bigtable.client.Client`` and
    ``kvdbclient.hbase.client.Client``. Every primitive currently raises
    ``NotImplementedError``; they are filled in by later ``pcgvast`` slices.
    """

    def __init__(
        self,
        table_id: str,
        config: VastConfig = VastConfig(),
        table_meta=None,
        lock_expiry: timedelta = timedelta(minutes=3),
    ):
        self._config = config
        self._table_id = table_id
        self._session = None  # lazy vastdb session; see _connect()
        self._init_common(
            logger_name=f"vast/{table_id}",
            table_meta=table_meta,
            lock_expiry=lock_expiry,
            max_row_key_count=config.MAX_ROW_KEY_COUNT,
        )

    def _connect(self):
        """Lazily open the VAST-DB session (imports ``vastdb`` on first use)."""
        if self._session is None:
            import vastdb  # noqa: F401  (optional ``vast`` extra)

            raise NotImplementedError(_PENDING)
        return self._session

    def close(self):
        """Release backend resources."""
        self._session = None

    # ── Abstract: backend-specific primitives (SimpleClient) ──────────────

    def create_table(self, meta, version: str, column_families=None) -> None:
        raise NotImplementedError(_PENDING)

    def mutate_row(self, row_key, val_dict, time_stamp=None):
        raise NotImplementedError(_PENDING)

    def _write_rows(self, rows, slow_retry=True, block_size=2000):
        raise NotImplementedError(_PENDING)

    def lock_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING)

    def lock_root_indefinitely(self, node_id, operation_id):
        raise NotImplementedError(_PENDING)

    def unlock_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING)

    def unlock_indefinitely_locked_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING)

    def renew_lock(self, node_id, operation_id):
        raise NotImplementedError(_PENDING)

    def lock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def lock_by_row_key_with_indefinite(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def lock_by_row_key_indefinitely(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def unlock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def unlock_indefinitely_locked_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def renew_lock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING)

    def get_compatible_timestamp(self, time_stamp, round_up=False):
        raise NotImplementedError(_PENDING)

    def read_all_rows(self):
        raise NotImplementedError(_PENDING)

    def create_column_family(self, family_id, gc_rule=None):
        raise NotImplementedError(_PENDING)

    def delete_cells(self, mutations, row_keys_to_delete=None):
        raise NotImplementedError(_PENDING)

    def delete_row(self, row_key):
        raise NotImplementedError(_PENDING)

    def _read_byte_row(
        self,
        row_key,
        columns=None,
        start_time=None,
        end_time=None,
        end_time_inclusive=False,
    ):
        raise NotImplementedError(_PENDING)

    def _read_byte_rows(
        self,
        start_key=None,
        end_key=None,
        end_key_inclusive=False,
        row_keys=None,
        columns=None,
        start_time=None,
        end_time=None,
        end_time_inclusive=False,
        user_id=None,
    ):
        raise NotImplementedError(_PENDING)

    def _delete_meta(self):
        raise NotImplementedError(_PENDING)

    # ── Abstract: ID generation (ClientWithIDGen) ─────────────────────────

    def _get_ids_range(self, key: bytes, size: int) -> typing.Tuple:
        raise NotImplementedError(_PENDING)
