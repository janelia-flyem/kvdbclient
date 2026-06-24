#!/usr/bin/env python3
"""Minimal VAST DB SDK probes for sorted table creation failures.

Run from repos/kvdbclient with credentials in the environment:

    pixi run python scripts/repro_vast_sorted_create_table.py
    pixi run python scripts/repro_vast_sorted_create_table.py --probe projection
    pixi run python scripts/repro_vast_sorted_create_table.py --probe both

Required environment:
    VAST_ENDPOINT
    VAST_ACCESS_KEY
    VAST_SECRET_KEY
    VAST_BUCKET

Optional environment:
    VAST_TEST_SCHEMA              preferred schema override
    VAST_SCHEMA                   fallback schema override
    VAST_TABLE_NAME               defaults to a unique test_ name
    VAST_ALLOW_PROTECTED_SCHEMA   set to 1 to allow pcgvast/autoproof/etc.

The failing call is:
    schema.create_table(..., sorting_key=["row_key"])

The projection probe tests the separate semi-sorted projection path. On this
cluster its DDL succeeds but reads never see the inserted rows, so the probe
also reproduces that second failure: it creates an unsorted table, adds

    table.create_projection(
        "by_row_key",
        sorted_columns=["row_key"],
        unsorted_columns=["family", "qualifier", "ts", "value"],
    )

inserts rows, waits for projection maintenance, then reads the table twice ---
once forced off the projection (QueryConfig(use_semi_sorted_projections=False))
and once forced onto it (use_semi_sorted_projections=True,
semi_sorted_projection_name="by_row_key"). The bug is reproduced when the
base-table read returns the rows while the forced-projection read (and
projection.stats.num_rows) returns zero. Tune with --num-row-keys and
--settle-seconds.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta
from importlib import metadata

import pyarrow as pa
import vastdb
from vastdb.config import QueryConfig


SORTING_KEY = ["row_key"]
UNSORTED_PROJECTION_COLUMNS = ["family", "qualifier", "ts", "value"]
PROTECTED_SCHEMAS = {"autoproof", "pcgvast", "pychunkedgraph"}


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def _package_version(name: str) -> str:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "unknown"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe VAST DB sorted-table and projection support."
    )
    parser.add_argument(
        "--probe",
        choices=("sorted", "projection", "both"),
        default=os.environ.get("VAST_PROBE", "sorted"),
        help="which probe to run; default: sorted",
    )
    parser.add_argument(
        "--num-row-keys",
        type=int,
        default=int(os.environ.get("VAST_PROBE_ROW_KEYS", "1024")),
        help="rows to insert for the projection read probe; default: 1024",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=float(os.environ.get("VAST_PROBE_SETTLE_SECONDS", "5.0")),
        help="seconds to wait for projection maintenance before reading; default: 5.0",
    )
    return parser.parse_args()


def _cells_schema() -> pa.Schema:
    return pa.schema(
        [
            ("row_key", pa.utf8()),
            ("family", pa.utf8()),
            ("qualifier", pa.binary()),
            ("ts", pa.timestamp("us")),
            ("value", pa.binary()),
        ]
    )


def _print_feature_gate(name: str, check) -> None:
    try:
        check()
    except Exception as exc:
        print(f"{name}: blocked by SDK feature gate: {exc!r}")
    else:
        print(f"{name}: passes SDK feature gate")


def _print_exception(exc: Exception) -> None:
    print(f"exception type: {type(exc).__module__}.{type(exc).__name__}")
    print(f"exception: {exc!r}")
    print()
    traceback.print_exc()


def _cleanup_table(session, bucket_name: str, schema_name: str, table_name: str) -> None:
    try:
        with session.transaction() as tx:
            schema = tx.bucket(bucket_name).schema(schema_name, fail_if_missing=False)
            if schema is None:
                return
            table = schema.table(table_name, fail_if_missing=False)
            if table is not None:
                table.drop()
                print(f"Dropped leftover test table: {table_name}")
    except Exception as exc:
        print(
            f"WARNING: cleanup failed for {bucket_name}/{schema_name}/{table_name}: "
            f"{exc!r}"
        )


def _probe_sorted_create_table(
    session,
    bucket_name: str,
    schema_name: str,
    table_name: str,
    columns: pa.Schema,
) -> bool:
    created = False
    try:
        with session.transaction() as tx:
            schema = tx.bucket(bucket_name).create_schema(
                schema_name, fail_if_exists=False
            )

            print("Calling schema.create_table(..., sorting_key=['row_key'])")
            table = schema.create_table(
                table_name,
                columns,
                fail_if_exists=True,
                sorting_key=SORTING_KEY,
            )
            created = True
            print("UNEXPECTED SUCCESS: sorted table was created.")
            print(f"created table handle: {table}")

            table.drop()
            created = False
            print("Dropped the unexpected sorted test table.")
            return True
    except Exception as exc:
        print("SORTED CREATE_TABLE FAILURE REPRODUCED")
        _print_exception(exc)
        return False
    finally:
        if created:
            _cleanup_table(session, bucket_name, schema_name, table_name)


def _make_probe_rows(columns: pa.Schema, num_row_keys: int) -> pa.Table:
    base_time = datetime(2026, 1, 1)
    row_key, family, qualifier, ts, value = [], [], [], [], []
    for index in range(num_row_keys):
        key = f"{index:020d}"
        row_key.append(key)
        family.append("0")
        qualifier.append(b"children")
        ts.append(base_time + timedelta(microseconds=index))
        value.append(f"v:{key}".encode("ascii"))
    return pa.table(
        {
            "row_key": row_key,
            "family": family,
            "qualifier": qualifier,
            "ts": ts,
            "value": value,
        },
        schema=columns,
    )


def _read_row_count(table, use_projection: bool) -> int:
    if use_projection:
        config = QueryConfig(
            use_semi_sorted_projections=True,
            semi_sorted_projection_name="by_row_key",
        )
    else:
        config = QueryConfig(use_semi_sorted_projections=False)
    return table.select(
        columns=list(table.columns().names),
        predicate=None,
        config=config,
    ).read_all().num_rows


def _probe_projection(
    session,
    bucket_name: str,
    schema_name: str,
    table_name: str,
    columns: pa.Schema,
    num_row_keys: int,
    settle_seconds: float,
) -> bool:
    created = False
    try:
        # 1) DDL: unsorted table + semi-sorted projection, then insert rows.
        with session.transaction() as tx:
            schema = tx.bucket(bucket_name).create_schema(
                schema_name, fail_if_exists=False
            )

            print("Calling schema.create_table(...) without sorting_key")
            table = schema.create_table(table_name, columns, fail_if_exists=True)
            created = True

            print(
                "Calling table.create_projection('by_row_key', "
                "sorted_columns=['row_key'], unsorted_columns=['family', "
                "'qualifier', 'ts', 'value'])"
            )
            projection = table.create_projection(
                "by_row_key",
                sorted_columns=SORTING_KEY,
                unsorted_columns=UNSORTED_PROJECTION_COLUMNS,
            )
            print(f"PROJECTION DDL SUCCESS: created {projection}")

            batch = _make_probe_rows(columns, num_row_keys)
            table.insert(batch)
            print(f"inserted {batch.num_rows} rows")

        # 2) Let projection maintenance run, then read it back.
        if settle_seconds > 0:
            print(f"waiting {settle_seconds:.1f}s for projection maintenance")
            time.sleep(settle_seconds)

        with session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
            stats = table.projection("by_row_key").stats
            stats_rows = stats.num_rows if stats is not None else None
            base_rows = _read_row_count(table, use_projection=False)
            projection_rows = _read_row_count(table, use_projection=True)

        print(f"projection.stats.num_rows:    {stats_rows}")
        print(f"base-table read rows:         {base_rows}")
        print(f"forced-projection read rows:  {projection_rows}")

        if base_rows > 0 and projection_rows == 0:
            print(
                "PROJECTION READ BUG REPRODUCED: base-table read returns rows but "
                "the forced semi-sorted projection read returns zero "
                "(projection never materializes the inserted rows)."
            )
            return False
        if projection_rows == base_rows:
            print(
                "PROJECTION READ OK: forced-projection read matches the base-table "
                "row count (zero-rows bug did NOT reproduce on this cluster)."
            )
            return True
        print(
            f"PROJECTION READ MISMATCH: base={base_rows} forced-projection="
            f"{projection_rows} (unexpected partial result)."
        )
        return False
    except Exception as exc:
        print("PROJECTION PROBE FAILURE")
        _print_exception(exc)
        return False
    finally:
        if created:
            _cleanup_table(session, bucket_name, schema_name, table_name)


def main() -> int:
    args = _parse_args()
    endpoint = _required_env("VAST_ENDPOINT")
    access_key = _required_env("VAST_ACCESS_KEY")
    secret_key = _required_env("VAST_SECRET_KEY")
    bucket_name = _required_env("VAST_BUCKET")
    schema_name = (
        os.environ.get("VAST_TEST_SCHEMA")
        or os.environ.get("VAST_SCHEMA")
        or "pcgvast_test"
    )
    table_name = os.environ.get(
        "VAST_TABLE_NAME", f"test_sorted_create_table_{uuid.uuid4().hex[:12]}"
    )

    if (
        schema_name in PROTECTED_SCHEMAS
        and os.environ.get("VAST_ALLOW_PROTECTED_SCHEMA") != "1"
    ):
        raise SystemExit(
            f"refusing to create a repro table in protected schema {schema_name!r}; "
            "set VAST_TEST_SCHEMA=pcgvast_test or VAST_ALLOW_PROTECTED_SCHEMA=1"
        )

    columns = _cells_schema()

    print("VAST sorted/projection support probe")
    print(f"vastdb package: {_package_version('vastdb')}")
    print(f"pyarrow package: {pa.__version__}")
    print(f"endpoint: {endpoint}")
    print(f"bucket: {bucket_name}")
    print(f"schema: {schema_name}")
    print(f"table prefix: {table_name}")
    print(f"columns: {columns}")
    print(f"sorting_key: {SORTING_KEY}")
    print(f"probe: {args.probe}")
    print()

    session = vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)
    print(f"server version reported by SDK: {session.features.vast_version}")
    _print_feature_gate(
        "semi-sorted projection", session.features.check_enforce_semisorted_projection
    )
    _print_feature_gate("Elysium sorted table", session.features.check_elysium)
    print()

    results = []
    if args.probe in {"sorted", "both"}:
        sorted_table_name = table_name if args.probe == "sorted" else f"{table_name}_sorted"
        results.append(
            _probe_sorted_create_table(
                session,
                bucket_name,
                schema_name,
                sorted_table_name,
                columns,
            )
        )
        print()

    if args.probe in {"projection", "both"}:
        projection_table_name = (
            table_name if args.probe == "projection" else f"{table_name}_projection"
        )
        results.append(
            _probe_projection(
                session,
                bucket_name,
                schema_name,
                projection_table_name,
                columns,
                num_row_keys=args.num_row_keys,
                settle_seconds=args.settle_seconds,
            )
        )

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
