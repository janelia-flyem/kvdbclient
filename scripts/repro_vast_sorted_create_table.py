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

The projection probe tests the separate semi-sorted projection path:
    table.create_projection(
        "by_row_key",
        sorted_columns=["row_key"],
        unsorted_columns=["family", "qualifier", "ts", "value"],
    )
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
import uuid
from importlib import metadata

import pyarrow as pa
import vastdb


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


def _probe_projection(
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
            print("PROJECTION SUCCESS: semi-sorted projection was created.")
            print(f"created projection handle: {projection}")

            table.drop()
            created = False
            print("Dropped the projection test table.")
            return True
    except Exception as exc:
        print("PROJECTION FAILURE REPRODUCED")
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
            )
        )

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
