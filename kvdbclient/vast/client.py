"""
VAST / VAST-DB backend client for kvdbclient.

The live read/write/meta primitives store kvdbclient's wide-column contract in
one append-only VAST table per ``table_id``. CAS locks and atomic ID counters
remain deferred to the pcgvast LockProvider/Sequencer slice.
"""

import typing
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from . import VastConfig
from . import utils as vast_utils
from .. import attributes
from .. import exceptions
from ..base import ClientWithIDGen
from ..base import ColumnFamilyConfig
from ..base import OperationLogger
from ..base import Cell

_PENDING_LOCKS = "VAST backend locks/ID generation deferred to LockProvider/Sequencer"


@dataclass(frozen=True)
class VastMutation:
    row_key: bytes
    cells: tuple[dict, ...]


class _PartialRowAdapter:
    """Compatibility wrapper for ``read_all_rows()`` rows."""

    __slots__ = ("cells",)

    def __init__(self, raw_cells):
        families = defaultdict(lambda: defaultdict(list))
        for cell in sorted(
            raw_cells,
            key=lambda row: vast_utils.from_vast_timestamp(row[vast_utils.TIMESTAMP]),
            reverse=True,
        ):
            families[cell[vast_utils.FAMILY]][cell[vast_utils.QUALIFIER]].append(
                Cell(
                    value=cell[vast_utils.VALUE],
                    timestamp=vast_utils.from_vast_timestamp(cell[vast_utils.TIMESTAMP]),
                )
            )
        self.cells = {family: dict(qualifiers) for family, qualifiers in families.items()}


class _ReadAllRowsResult:
    """BigTable-like result wrapper exposing ``.rows`` and ``consume_all()``."""

    __slots__ = ("rows",)

    def __init__(self, raw_cells):
        by_row = defaultdict(list)
        for cell in raw_cells:
            by_row[vast_utils.decode_row_key(cell[vast_utils.ROW_KEY])].append(cell)
        self.rows = {
            row_key: _PartialRowAdapter(cells) for row_key, cells in by_row.items()
        }

    def consume_all(self):
        pass


class Client(ClientWithIDGen, OperationLogger):
    """Key-value backend over VAST-DB."""

    def __init__(
        self,
        table_id: str,
        config: VastConfig = VastConfig(),
        table_meta=None,
        lock_expiry: timedelta = timedelta(minutes=3),
    ):
        self._config = config
        self._table_id = table_id
        self._session = None
        self._init_common(
            logger_name=f"vast/{table_id}",
            table_meta=table_meta,
            lock_expiry=lock_expiry,
            max_row_key_count=config.MAX_ROW_KEY_COUNT,
        )

    def _validate_config(self):
        missing = [
            name
            for name in ("ENDPOINT", "ACCESS_KEY", "SECRET_KEY", "BUCKET", "SCHEMA")
            if not getattr(self._config, name)
        ]
        if missing:
            raise ValueError(f"incomplete VAST config: missing {missing}")

    def _connect(self):
        """Lazily open a VAST-DB session and ensure the configured schema."""
        if self._session is None:
            self._validate_config()
            import vastdb

            self._session = vastdb.connect(
                endpoint=self._config.ENDPOINT,
                access=self._config.ACCESS_KEY,
                secret=self._config.SECRET_KEY,
            )
            with self._session.transaction() as tx:
                tx.bucket(self._config.BUCKET).create_schema(
                    self._config.SCHEMA, fail_if_exists=False
                )
        return self._session

    def close(self):
        self._session = None

    def _schema(self, tx):
        bucket = tx.bucket(self._config.BUCKET)
        return bucket.create_schema(self._config.SCHEMA, fail_if_exists=False)

    def _table(self, tx, fail_if_missing=True):
        return self._schema(tx).table(self._table_id, fail_if_missing=fail_if_missing)

    def _validate_table(self, table):
        expected = vast_utils.cells_schema()
        actual = table.columns()
        if actual.names != expected.names:
            raise ValueError(
                f"VAST table schema mismatch: names {actual.names} != {expected.names}"
            )
        for field in expected:
            actual_field = actual.field(field.name)
            if actual_field.type != field.type:
                raise ValueError(
                    f"VAST table column {field.name!r} has type "
                    f"{actual_field.type}, expected {field.type}"
                )

        if not self._config.SORTED:
            return

        sorted_names = [field.name for field in table.sorted_columns()]
        if sorted_names[:1] != vast_utils.SORTING_KEY:
            raise ValueError(
                f"VAST table must be sorted on {vast_utils.SORTING_KEY}, got {sorted_names}"
            )

    def create_table(
        self,
        meta,
        version: str,
        column_families=None,
        fail_if_exists: bool = True,
    ) -> None:
        session = self._connect()
        created = False
        with session.transaction() as tx:
            if self._config.SORTED:
                tx._rpc.features.check_elysium()
            schema = self._schema(tx)
            table = schema.table(self._table_id, fail_if_missing=False)
            if table is not None:
                if fail_if_exists:
                    raise ValueError(f"{self._table_id} already exists.")
            else:
                table = schema.create_table(
                    self._table_id,
                    vast_utils.cells_schema(),
                    fail_if_exists=True,
                    sorting_key=(
                        vast_utils.SORTING_KEY if self._config.SORTED else []
                    ),
                )
                created = True
            self._validate_table(table)

        if created:
            self.add_table_version(version)
            self.update_table_meta(meta)

    def create_column_family(self, family_id, gc_rule=None):
        if isinstance(family_id, ColumnFamilyConfig):
            family_id, gc_rule = family_id.family_id, family_id

        meta = self.read_table_meta()
        if not isinstance(meta, dict):
            return

        updated = dict(meta)
        families = dict(updated.get("_vast_column_families", {}))
        entry = {}
        if isinstance(gc_rule, ColumnFamilyConfig):
            if gc_rule.max_versions is not None:
                entry["max_versions"] = gc_rule.max_versions
            if gc_rule.max_age is not None:
                entry["max_age_seconds"] = int(gc_rule.max_age.total_seconds())
        elif gc_rule is not None:
            if hasattr(gc_rule, "max_num_versions"):
                entry["max_versions"] = gc_rule.max_num_versions
            if hasattr(gc_rule, "max_age"):
                entry["max_age_seconds"] = int(gc_rule.max_age.total_seconds())
        families[str(family_id)] = entry
        updated["_vast_column_families"] = families
        self.update_table_meta(updated, overwrite=True)

    def mutate_row(
        self,
        row_key: bytes,
        val_dict: typing.Dict[attributes._Attribute, typing.Any],
        time_stamp: typing.Optional[datetime] = None,
    ) -> VastMutation:
        if time_stamp is None:
            time_stamp = datetime.now(timezone.utc)
        time_stamp = vast_utils.get_vast_compatible_time_stamp(time_stamp)
        encoded_row_key = vast_utils.encode_row_key(row_key)
        cells = []
        for column, value in val_dict.items():
            cells.append(
                {
                    vast_utils.ROW_KEY: encoded_row_key,
                    vast_utils.FAMILY: column.family_id,
                    vast_utils.QUALIFIER: column.key,
                    vast_utils.TIMESTAMP: time_stamp,
                    vast_utils.VALUE: column.serialize(value),
                }
            )
        return VastMutation(row_key=row_key, cells=tuple(cells))

    def _write_rows(self, rows, slow_retry=True, block_size=2000):
        del slow_retry
        cell_rows = []
        for row in rows:
            cell_rows.extend(row.cells)
        if not cell_rows:
            return

        with self._connect().transaction() as tx:
            table = self._table(tx)
            for chunk in vast_utils.chunked(cell_rows, block_size):
                table.insert(vast_utils.make_record_batch(chunk))

    def get_compatible_timestamp(
        self, time_stamp: datetime, round_up: bool = False
    ) -> datetime:
        return vast_utils.get_vast_compatible_time_stamp(
            time_stamp, round_up=round_up
        )

    def _select_rows_once(
        self,
        *,
        row_keys=None,
        start_key=None,
        end_key=None,
        end_key_inclusive=False,
        columns=None,
        start_time=None,
        end_time=None,
        end_time_inclusive=False,
    ):
        with self._connect().transaction() as tx:
            table = self._table(tx)
            predicate = vast_utils.build_predicate(
                table,
                row_keys=row_keys,
                start_key=start_key,
                end_key=end_key,
                end_key_inclusive=end_key_inclusive,
                columns=columns,
                start_time=start_time,
                end_time=end_time,
                end_time_inclusive=end_time_inclusive,
            )
            return table.select(
                columns=list(vast_utils.CELL_FIELD_NAMES),
                predicate=predicate,
            ).read_all().to_pylist()

    def _matching_user_row_keys(
        self,
        *,
        row_keys=None,
        start_key=None,
        end_key=None,
        end_key_inclusive=False,
        user_id=None,
        start_time=None,
        end_time=None,
        end_time_inclusive=False,
    ):
        with self._connect().transaction() as tx:
            table = self._table(tx)
            predicate = vast_utils._and_predicates(
                [
                    vast_utils.row_key_predicate(
                        table,
                        row_keys=row_keys,
                        start_key=start_key,
                        end_key=end_key,
                        end_key_inclusive=end_key_inclusive,
                    ),
                    vast_utils.time_predicate(
                        table,
                        start_time=start_time,
                        end_time=end_time,
                        end_time_inclusive=end_time_inclusive,
                    ),
                    vast_utils.value_predicate(
                        table, attributes.OperationLogs.UserID, user_id
                    ),
                ]
            )
            rows = table.select(
                columns=[vast_utils.ROW_KEY],
                predicate=predicate,
            ).read_all().to_pylist()
        return sorted({vast_utils.decode_row_key(row[vast_utils.ROW_KEY]) for row in rows})

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
        single_column = columns if isinstance(columns, attributes._Attribute) else None

        if row_keys is not None:
            row_keys = list(row_keys)
        elif start_key is None or end_key is None:
            raise exceptions.PreconditionError(
                "Need to either provide a valid set of rows, or both, a start row and an end row."
            )

        if user_id is not None:
            row_keys = self._matching_user_row_keys(
                row_keys=row_keys,
                start_key=start_key,
                end_key=end_key,
                end_key_inclusive=end_key_inclusive,
                user_id=user_id,
                start_time=start_time,
                end_time=end_time,
                end_time_inclusive=end_time_inclusive,
            )
            start_key = end_key = None
            end_key_inclusive = False

        raw_rows = []
        if row_keys is not None:
            for chunk in vast_utils.chunked(row_keys, self._max_row_key_count):
                raw_rows.extend(
                    self._select_rows_once(
                        row_keys=chunk,
                        columns=columns,
                        start_time=start_time,
                        end_time=end_time,
                        end_time_inclusive=end_time_inclusive,
                    )
                )
        else:
            raw_rows = self._select_rows_once(
                start_key=start_key,
                end_key=end_key,
                end_key_inclusive=end_key_inclusive,
                columns=columns,
                start_time=start_time,
                end_time=end_time,
                end_time_inclusive=end_time_inclusive,
            )

        return vast_utils.rows_to_column_dicts(
            raw_rows,
            columns=vast_utils.normalize_columns(columns),
            single_column=single_column,
            deserialize=True,
        )

    def _read_byte_row(
        self,
        row_key,
        columns=None,
        start_time=None,
        end_time=None,
        end_time_inclusive=False,
    ):
        single_column = isinstance(columns, attributes._Attribute)
        rows = self._read_byte_rows(
            row_keys=[row_key],
            columns=columns,
            start_time=start_time,
            end_time=end_time,
            end_time_inclusive=end_time_inclusive,
        )
        return rows.get(row_key, [] if single_column else {})

    def read_all_rows(self):
        raw_rows = self._select_rows_once()
        return _ReadAllRowsResult(raw_rows)

    def _delete_matching(self, table, predicate):
        rows = table.select(
            columns=[vast_utils.ROW_KEY],
            predicate=predicate,
            internal_row_id=True,
        ).read_all()
        if rows.num_rows:
            table.delete(rows)

    def _delete_meta(self):
        self.delete_row(attributes.TableMeta.key)

    def delete_cells(self, mutations, row_keys_to_delete=None):
        with self._connect().transaction() as tx:
            table = self._table(tx)
            for row_key, column, timestamps in mutations:
                for ts in timestamps:
                    predicate = vast_utils.build_predicate(
                        table,
                        row_keys=[row_key],
                        columns=column,
                        start_time=ts,
                        end_time=ts,
                        end_time_inclusive=True,
                    )
                    self._delete_matching(table, predicate)

            for row_key in row_keys_to_delete or []:
                predicate = vast_utils.build_predicate(table, row_keys=[row_key])
                self._delete_matching(table, predicate)

    def delete_row(self, row_key):
        with self._connect().transaction() as tx:
            table = self._table(tx)
            predicate = vast_utils.build_predicate(table, row_keys=[row_key])
            self._delete_matching(table, predicate)

    # -- Deferred lock primitives -------------------------------------------------

    def lock_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def lock_root_indefinitely(self, node_id, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def unlock_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def unlock_indefinitely_locked_root(self, node_id, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def renew_lock(self, node_id, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def lock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def lock_by_row_key_with_indefinite(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def lock_by_row_key_indefinitely(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def unlock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def unlock_indefinitely_locked_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    def renew_lock_by_row_key(self, row_key, operation_id):
        raise NotImplementedError(_PENDING_LOCKS)

    # -- Deferred ID generation ---------------------------------------------------

    def _get_ids_range(self, key: bytes, size: int) -> typing.Tuple:
        raise NotImplementedError(_PENDING_LOCKS)
