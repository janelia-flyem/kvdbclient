"""VAST-specific helpers for the kvdbclient wide-column contract."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Iterable, Optional, Sequence

from .. import attributes
from ..base import Cell

ROW_KEY = "row_key"
FAMILY = "family"
QUALIFIER = "qualifier"
TIMESTAMP = "ts"
VALUE = "value"

CELL_FIELD_NAMES = (ROW_KEY, FAMILY, QUALIFIER, TIMESTAMP, VALUE)
SORTING_KEY = [ROW_KEY]


def cells_schema():
    import pyarrow as pa

    return pa.schema(
        [
            (ROW_KEY, pa.utf8()),
            (FAMILY, pa.utf8()),
            (QUALIFIER, pa.binary()),
            (TIMESTAMP, pa.timestamp("us")),
            (VALUE, pa.binary()),
        ]
    )


def encode_row_key(row_key: bytes) -> str:
    if not isinstance(row_key, bytes):
        raise TypeError(f"row_key must be bytes, got {type(row_key)!r}")
    return row_key.decode("utf-8", errors="strict")


def decode_row_key(row_key: str) -> bytes:
    if not isinstance(row_key, str):
        raise TypeError(f"row_key must be str, got {type(row_key)!r}")
    return row_key.encode("utf-8", errors="strict")


def ensure_utc(time_stamp: datetime) -> datetime:
    if time_stamp.tzinfo is None:
        return time_stamp.replace(tzinfo=timezone.utc)
    return time_stamp.astimezone(timezone.utc)


def get_vast_compatible_time_stamp(
    time_stamp: datetime, round_up: bool = False
) -> datetime:
    time_stamp = ensure_utc(time_stamp)
    microsecond_gap = timedelta(microseconds=time_stamp.microsecond % 1000)
    if microsecond_gap == timedelta(0):
        if round_up:
            return time_stamp + timedelta(milliseconds=1)
        return time_stamp
    if round_up:
        return time_stamp + (timedelta(milliseconds=1) - microsecond_gap)
    return time_stamp - microsecond_gap


def to_vast_timestamp(time_stamp: datetime) -> datetime:
    return ensure_utc(time_stamp).replace(tzinfo=None)


def from_vast_timestamp(time_stamp: datetime) -> datetime:
    if time_stamp.tzinfo is None:
        return time_stamp.replace(tzinfo=timezone.utc)
    return time_stamp.astimezone(timezone.utc)


def attribute_to_columns(column: attributes._Attribute) -> tuple[str, bytes]:
    return column.family_id, column.key


def attribute_from_columns(family: str, qualifier: bytes) -> attributes._Attribute:
    return attributes.from_key(family, qualifier)


def normalize_columns(columns) -> Optional[tuple[attributes._Attribute, ...]]:
    if columns is None:
        return None
    if isinstance(columns, attributes._Attribute):
        return (columns,)
    columns = tuple(columns)
    if not columns:
        raise ValueError(
            f"Empty column filter {columns} is ambiguous. Pass None if no column filter should be applied."
        )
    return columns


def chunked(values: Iterable, size: int):
    values = iter(values)
    while True:
        chunk = list(islice(values, size))
        if not chunk:
            return
        yield chunk


def make_record_batch(cell_rows: Sequence[dict]):
    import pyarrow as pa

    schema = cells_schema()
    data = {name: [] for name in CELL_FIELD_NAMES}
    for row in cell_rows:
        data[ROW_KEY].append(row[ROW_KEY])
        data[FAMILY].append(row[FAMILY])
        data[QUALIFIER].append(row[QUALIFIER])
        data[TIMESTAMP].append(to_vast_timestamp(row[TIMESTAMP]))
        data[VALUE].append(row[VALUE])
    return pa.record_batch([data[name] for name in CELL_FIELD_NAMES], schema=schema)


def _and_predicates(predicates):
    predicates = [predicate for predicate in predicates if predicate is not None]
    if not predicates:
        return None
    predicate = predicates[0]
    for next_predicate in predicates[1:]:
        predicate = predicate & next_predicate
    return predicate


def row_key_predicate(
    table,
    *,
    row_keys=None,
    start_key=None,
    end_key=None,
    end_key_inclusive=False,
):
    row_key_col = table[ROW_KEY]
    if row_keys is not None:
        row_keys = [encode_row_key(row_key) for row_key in row_keys]
        if not row_keys:
            return False
        if len(row_keys) == 1:
            return row_key_col == row_keys[0]
        return row_key_col.isin(row_keys)

    if start_key is None or end_key is None:
        return None

    start = encode_row_key(start_key)
    end = encode_row_key(end_key)
    if end_key_inclusive:
        return (row_key_col >= start) & (row_key_col <= end)
    return (row_key_col >= start) & (row_key_col < end)


def column_predicate(table, columns=None):
    columns = normalize_columns(columns)
    if columns is None:
        return None
    # VAST's predicate serializer cannot express an OR of (family AND qualifier)
    # clauses, so only a single-column filter (a plain AND) is pushed down.
    # Multi-column requests are filtered client-side in rows_to_column_dicts.
    if len(columns) != 1:
        return None
    family, qualifier = attribute_to_columns(columns[0])
    return (table[FAMILY] == family) & (table[QUALIFIER] == qualifier)


def time_predicate(
    table,
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    end_time_inclusive: bool = False,
):
    predicates = []
    if start_time is not None:
        start_time = get_vast_compatible_time_stamp(start_time)
        predicates.append(table[TIMESTAMP] >= to_vast_timestamp(start_time))
    if end_time is not None:
        end_time = get_vast_compatible_time_stamp(end_time)
        vast_end = to_vast_timestamp(end_time)
        if end_time_inclusive:
            predicates.append(table[TIMESTAMP] <= vast_end)
        else:
            predicates.append(table[TIMESTAMP] < vast_end)
    return _and_predicates(predicates)


def value_predicate(table, column: attributes._Attribute, value):
    return (
        (table[FAMILY] == column.family_id)
        & (table[QUALIFIER] == column.key)
        & (table[VALUE] == column.serialize(value))
    )


def build_predicate(
    table,
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
    return _and_predicates(
        [
            row_key_predicate(
                table,
                row_keys=row_keys,
                start_key=start_key,
                end_key=end_key,
                end_key_inclusive=end_key_inclusive,
            ),
            column_predicate(table, columns),
            time_predicate(
                table,
                start_time=start_time,
                end_time=end_time,
                end_time_inclusive=end_time_inclusive,
            ),
        ]
    )


def rows_to_column_dicts(rows, *, columns=None, single_column=None, deserialize=True):
    wanted = set(columns) if columns is not None else None
    grouped = defaultdict(lambda: defaultdict(list))
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row[ROW_KEY],
            row[FAMILY],
            row[QUALIFIER],
            from_vast_timestamp(row[TIMESTAMP]),
        ),
        reverse=True,
    )
    for row in sorted_rows:
        try:
            attr = attribute_from_columns(row[FAMILY], row[QUALIFIER])
        except KeyError:
            continue
        if wanted is not None and attr not in wanted:
            continue
        value = row[VALUE]
        if deserialize:
            value = attr.deserialize(value)
        grouped[decode_row_key(row[ROW_KEY])][attr].append(
            Cell(value=value, timestamp=from_vast_timestamp(row[TIMESTAMP]))
        )

    result = {}
    for row_key, column_dict in grouped.items():
        if single_column is not None:
            result[row_key] = column_dict.get(single_column, [])
        else:
            result[row_key] = dict(column_dict)
    return result
