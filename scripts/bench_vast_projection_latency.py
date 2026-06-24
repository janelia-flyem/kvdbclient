#!/usr/bin/env python3
"""Benchmark VAST DB semi-sorted projection latency against baseline scans.

This drives the raw VAST DB Python SDK directly. It creates an unsorted test
table, adds a semi-sorted projection on row_key, inserts synthetic kvdbclient
cell rows, then compares identical predicates with:

    QueryConfig(use_semi_sorted_projections=False)
    QueryConfig(use_semi_sorted_projections=True,
                semi_sorted_projection_name="by_row_key")

Run from repos/kvdbclient:

    VAST_TEST_SCHEMA=pcgvast_test pixi run python \\
        scripts/bench_vast_projection_latency.py --row-keys 10000
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import math
import os
import random
import statistics
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib import metadata
from pathlib import Path
from typing import Callable, Iterable

import pyarrow as pa
import vastdb
from vastdb.config import QueryConfig


PROJECTION_NAME = "by_row_key"
PROTECTED_SCHEMAS = {"autoproof", "pcgvast", "pychunkedgraph"}
QUERY_COLUMNS = ["row_key", "family", "qualifier", "ts", "value"]
SPECIAL_KEYS = (
    "i00000000000000000000",
    "i00000000000000000001",
    "f00000000000000000000",
    "f00000000000000000001",
    "reserved_meta",
    "reserved_version",
)


@dataclass(frozen=True)
class QuerySpec:
    kind: str
    label: str
    build_predicate: Callable


@dataclass(frozen=True)
class QueryResult:
    elapsed_ms: float
    rows_returned: int
    row_key_set_hash: str
    query_data_calls: int
    projection_names: tuple[str, ...]
    sorted_projection_flags: tuple[bool, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark baseline vs semi-sorted projection VAST reads."
    )
    parser.add_argument("--row-keys", type=int, default=10_000)
    parser.add_argument("--cells-per-key", type=int, default=4)
    parser.add_argument("--samples", type=int, default=300)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--batch-row-keys", type=int, default=5_000)
    parser.add_argument("--range-width", type=int, default=16)
    parser.add_argument("--prefix-length", type=int, default=19)
    parser.add_argument("--value-bytes", type=int, default=32)
    parser.add_argument("--settle-seconds", type=float, default=3.0)
    parser.add_argument(
        "--projection-timing",
        choices=("before", "after"),
        default="before",
        help="create projection before or after inserting benchmark rows",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--num-splits", type=int, default=1)
    parser.add_argument(
        "--query-types",
        default="point,range,prefix",
        help="comma-separated subset of point,range,prefix",
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--table-name", default="")
    parser.add_argument("--keep-table", action="store_true")
    parser.add_argument(
        "--allow-protected-schema",
        action="store_true",
        help="allow schemas outside the fail-safe pcgvast_test path",
    )
    return parser.parse_args()


def load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def merged_env(env_file: str) -> dict[str, str]:
    env = dict(os.environ)
    for key, value in load_dotenv(Path(env_file)).items():
        env.setdefault(key, value)
    return env


def required(env: dict[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def package_version(name: str) -> str:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "unknown"


def cells_schema() -> pa.Schema:
    return pa.schema(
        [
            ("row_key", pa.utf8()),
            ("family", pa.utf8()),
            ("qualifier", pa.binary()),
            ("ts", pa.timestamp("us")),
            ("value", pa.binary()),
        ]
    )


def numeric_key(index: int) -> str:
    return f"{index:020d}"


def padded_value(row_key: str, cell_index: int, value_bytes: int) -> bytes:
    raw = f"v:{row_key}:{cell_index}".encode("ascii")
    if len(raw) >= value_bytes:
        return raw[:value_bytes]
    return raw + (b"x" * (value_bytes - len(raw)))


def make_cell_table(
    key_indices: Iterable[int],
    cells_per_key: int,
    value_bytes: int,
    special_keys: Iterable[str] = (),
) -> pa.Table:
    row_key: list[str] = []
    family: list[str] = []
    qualifier: list[bytes] = []
    ts: list[datetime] = []
    value: list[bytes] = []

    base_time = datetime(2026, 1, 1)
    keys = [numeric_key(index) for index in key_indices]
    keys.extend(special_keys)

    for logical_index, key in enumerate(keys):
        for cell_index in range(cells_per_key):
            row_key.append(key)
            family.append(f"cf{cell_index % 3}")
            qualifier.append(f"q{cell_index:03d}".encode("ascii"))
            ts.append(base_time + timedelta(microseconds=logical_index * 1000 + cell_index))
            value.append(padded_value(key, cell_index, value_bytes))

    return pa.Table.from_pydict(
        {
            "row_key": row_key,
            "family": family,
            "qualifier": qualifier,
            "ts": ts,
            "value": value,
        },
        schema=cells_schema(),
    )


def successor_for_digit_prefix(prefix: str) -> str:
    chars = list(prefix)
    for index in range(len(chars) - 1, -1, -1):
        if "0" <= chars[index] < "9":
            chars[index] = chr(ord(chars[index]) + 1)
            return "".join(chars[: index + 1])
    return prefix + "~"


def row_key_set_hash(table: pa.Table) -> str:
    keys = sorted(set(table.column("row_key").to_pylist()))
    digest = hashlib.sha256()
    for key in keys:
        digest.update(key.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]


@contextlib.contextmanager
def query_data_trace(api):
    api_cls = type(api)
    original = api_cls.query_data
    calls: list[dict] = []

    def wrapped(self, *args, **kwargs):
        calls.append(
            {
                "projection": kwargs.get("projection"),
                "enable_sorted_projections": kwargs.get("enable_sorted_projections"),
            }
        )
        return original(self, *args, **kwargs)

    api_cls.query_data = wrapped
    try:
        yield calls
    finally:
        api_cls.query_data = original


def query_config(arm: str, num_splits: int, query_id: str) -> QueryConfig:
    if arm == "baseline":
        return QueryConfig(
            num_splits=num_splits,
            use_semi_sorted_projections=False,
            query_id=query_id,
        )
    if arm == "projection":
        return QueryConfig(
            num_splits=num_splits,
            use_semi_sorted_projections=True,
            semi_sorted_projection_name=PROJECTION_NAME,
            query_id=query_id,
        )
    raise ValueError(f"unknown arm: {arm}")


def run_query(table, tx_api, spec: QuerySpec, arm: str, num_splits: int) -> QueryResult:
    config = query_config(arm, num_splits, f"{arm}-{spec.kind}")
    predicate = spec.build_predicate(table)
    start = time.perf_counter()
    with query_data_trace(tx_api) as calls:
        result = table.select(
            columns=QUERY_COLUMNS,
            predicate=predicate,
            config=config,
        ).read_all()
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    projections = tuple(str(call.get("projection") or "") for call in calls)
    flags = tuple(bool(call.get("enable_sorted_projections")) for call in calls)
    if arm == "baseline" and any(projections):
        raise RuntimeError(f"baseline unexpectedly requested projections: {projections}")
    if arm == "projection" and PROJECTION_NAME not in projections:
        raise RuntimeError(f"projection arm did not request {PROJECTION_NAME}: {projections}")

    return QueryResult(
        elapsed_ms=elapsed_ms,
        rows_returned=result.num_rows,
        row_key_set_hash=row_key_set_hash(result),
        query_data_calls=len(calls),
        projection_names=projections,
        sorted_projection_flags=flags,
    )


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "n": float(len(values)),
        "mean": statistics.fmean(values) if values else math.nan,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "min": min(values) if values else math.nan,
        "max": max(values) if values else math.nan,
    }


def print_summary(timings: dict[tuple[str, str], list[float]]) -> None:
    print()
    print("Latency summary (ms)")
    print("query,arm,n,mean,p50,p95,p99,min,max")
    for query_kind in sorted({kind for kind, _ in timings}):
        for arm in ("baseline", "projection"):
            stats = summarize(timings[(query_kind, arm)])
            print(
                f"{query_kind},{arm},{int(stats['n'])},"
                f"{stats['mean']:.3f},{stats['p50']:.3f},"
                f"{stats['p95']:.3f},{stats['p99']:.3f},"
                f"{stats['min']:.3f},{stats['max']:.3f}"
            )


def create_table(session, bucket_name: str, schema_name: str, table_name: str):
    with session.transaction() as tx:
        schema = tx.bucket(bucket_name).create_schema(schema_name, fail_if_exists=False)
        table = schema.create_table(table_name, cells_schema(), fail_if_exists=True)
        print(f"created table: {table_name}")


def create_projection(session, bucket_name: str, schema_name: str, table_name: str):
    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
        projection = table.create_projection(
            PROJECTION_NAME,
            sorted_columns=["row_key"],
            unsorted_columns=["family", "qualifier", "ts", "value"],
        )
        print(f"created projection: {projection.name}, initial_stats={projection.stats}")


def insert_rows(
    session,
    bucket_name: str,
    schema_name: str,
    table_name: str,
    row_keys: int,
    cells_per_key: int,
    batch_row_keys: int,
    value_bytes: int,
) -> None:
    inserted_rows = 0
    for start_index in range(0, row_keys, batch_row_keys):
        end_index = min(row_keys, start_index + batch_row_keys)
        batch = make_cell_table(
            range(start_index, end_index),
            cells_per_key=cells_per_key,
            value_bytes=value_bytes,
        )
        with session.transaction() as tx:
            table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
            table.insert(batch)
        inserted_rows += batch.num_rows
        print(
            f"inserted numeric keys {start_index}:{end_index} "
            f"({inserted_rows} physical rows total)"
        )

    special_batch = make_cell_table(
        (),
        cells_per_key=cells_per_key,
        value_bytes=value_bytes,
        special_keys=SPECIAL_KEYS,
    )
    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
        table.insert(special_batch)
    inserted_rows += special_batch.num_rows
    print(f"inserted {len(SPECIAL_KEYS)} special keys ({inserted_rows} physical rows total)")


def drop_test_table(session, bucket_name: str, schema_name: str, table_name: str) -> None:
    with session.transaction() as tx:
        schema = tx.bucket(bucket_name).schema(schema_name, fail_if_missing=False)
        if schema is None:
            return
        table = schema.table(table_name, fail_if_missing=False)
        if table is None:
            return
        try:
            projection = table.projection(PROJECTION_NAME)
            projection.drop()
        except Exception as exc:
            print(f"projection cleanup skipped or failed: {exc!r}")
        table.drop()
        print(f"dropped test table: {table_name}")


def sample_unique(rng: random.Random, population_size: int, count: int) -> list[int]:
    if population_size <= 0:
        raise ValueError("population_size must be positive")
    if count <= population_size:
        return rng.sample(range(population_size), count)
    return [rng.randrange(population_size) for _ in range(count)]


def build_query_specs(args: argparse.Namespace, rng: random.Random) -> list[QuerySpec]:
    requested = {item.strip() for item in args.query_types.split(",") if item.strip()}
    allowed = {"point", "range", "prefix"}
    unknown = requested - allowed
    if unknown:
        raise SystemExit(f"unknown query types: {sorted(unknown)}")

    total = args.samples + args.warmups
    specs: list[QuerySpec] = []

    if "point" in requested:
        for index in sample_unique(rng, args.row_keys, total):
            key = numeric_key(index)
            specs.append(
                QuerySpec(
                    "point",
                    key,
                    lambda table, key=key: table["row_key"] == key,
                )
            )

    if "range" in requested:
        max_start = max(1, args.row_keys - args.range_width)
        for start in sample_unique(rng, max_start, total):
            lower = numeric_key(start)
            upper = numeric_key(min(args.row_keys, start + args.range_width))
            specs.append(
                QuerySpec(
                    "range",
                    f"{lower}..{upper}",
                    lambda table, lower=lower, upper=upper: (
                        (table["row_key"] >= lower) & (table["row_key"] < upper)
                    ),
                )
            )

    if "prefix" in requested:
        prefixes = sorted(
            {numeric_key(index)[: args.prefix_length] for index in range(args.row_keys)}
        )
        for prefix_index in sample_unique(rng, len(prefixes), total):
            prefix = prefixes[prefix_index]
            upper = successor_for_digit_prefix(prefix)
            specs.append(
                QuerySpec(
                    "prefix",
                    f"{prefix}*",
                    lambda table, prefix=prefix, upper=upper: (
                        (table["row_key"] >= prefix) & (table["row_key"] < upper)
                    ),
                )
            )

    rng.shuffle(specs)
    return specs


def run_benchmark(
    session,
    bucket_name: str,
    schema_name: str,
    table_name: str,
    args: argparse.Namespace,
) -> dict[tuple[str, str], list[float]]:
    rng = random.Random(args.seed)
    specs = build_query_specs(args, rng)
    remaining_warmups = {kind: args.warmups for kind in {"point", "range", "prefix"}}
    timings: dict[tuple[str, str], list[float]] = {
        (kind, arm): []
        for kind in {"point", "range", "prefix"}
        for arm in {"baseline", "projection"}
    }

    with session.transaction() as tx:
        table = tx.bucket(bucket_name).schema(schema_name).table(table_name)
        projection = table.projection(PROJECTION_NAME)
        print(f"projection stats before benchmark: {projection.stats}")
        if projection.stats.num_rows == 0:
            raise RuntimeError(
                "projection stats report zero rows; increase --row-keys/--cells-per-key "
                "or --settle-seconds before benchmarking latency"
            )
        print(f"benchmark query pairs: {len(specs)}")

        for ordinal, spec in enumerate(specs, 1):
            arms = ["baseline", "projection"]
            rng.shuffle(arms)
            pair: dict[str, QueryResult] = {}
            for arm in arms:
                pair[arm] = run_query(table, tx._rpc.api, spec, arm, args.num_splits)

            base = pair["baseline"]
            proj = pair["projection"]
            if (
                base.rows_returned != proj.rows_returned
                or base.row_key_set_hash != proj.row_key_set_hash
            ):
                raise RuntimeError(
                    "baseline/projection result mismatch for "
                    f"{spec.kind}:{spec.label}: baseline={base}, projection={proj}"
                )

            is_warmup = remaining_warmups.get(spec.kind, 0) > 0
            if is_warmup:
                remaining_warmups[spec.kind] -= 1
            else:
                for arm, result in pair.items():
                    timings[(spec.kind, arm)].append(result.elapsed_ms)

            if ordinal % 50 == 0 or ordinal == len(specs):
                print(f"completed {ordinal}/{len(specs)} paired queries")

    return timings


def main() -> int:
    args = parse_args()
    env = merged_env(args.env_file)
    endpoint = required(env, "VAST_ENDPOINT")
    access = required(env, "VAST_ACCESS_KEY")
    secret = required(env, "VAST_SECRET_KEY")
    bucket_name = required(env, "VAST_BUCKET")
    schema_name = env.get("VAST_TEST_SCHEMA") or env.get("VAST_SCHEMA") or "pcgvast_test"
    table_name = args.table_name or f"test_projection_latency_{uuid.uuid4().hex[:12]}"

    if not table_name.startswith("test_"):
        raise SystemExit(f"refusing non-test table name: {table_name!r}")
    if (
        not args.allow_protected_schema
        and (schema_name != "pcgvast_test" or schema_name in PROTECTED_SCHEMAS)
    ):
        raise SystemExit(
            f"refusing schema {schema_name!r}; set VAST_TEST_SCHEMA=pcgvast_test "
            "or pass --allow-protected-schema"
        )

    print("VAST semi-sorted projection latency benchmark")
    print(f"vastdb package: {package_version('vastdb')}")
    print(f"pyarrow package: {pa.__version__}")
    print(f"endpoint: {endpoint}")
    print(f"bucket: {bucket_name}")
    print(f"schema: {schema_name}")
    print(f"table: {table_name}")
    print(f"row_keys: {args.row_keys}")
    print(f"cells_per_key: {args.cells_per_key}")
    print(
        "physical_rows: "
        f"{args.row_keys * args.cells_per_key + len(SPECIAL_KEYS) * args.cells_per_key}"
    )
    print(f"samples_per_query_type: {args.samples}")
    print(f"warmups_per_query_type: {args.warmups}")
    print(f"query_types: {args.query_types}")
    print()

    session = vastdb.connect(endpoint=endpoint, access=access, secret=secret)
    print(f"server version reported by SDK: {session.features.vast_version}")
    session.features.check_enforce_semisorted_projection()

    try:
        create_table(session, bucket_name, schema_name, table_name)
        if args.projection_timing == "before":
            create_projection(session, bucket_name, schema_name, table_name)
        insert_rows(
            session,
            bucket_name,
            schema_name,
            table_name,
            row_keys=args.row_keys,
            cells_per_key=args.cells_per_key,
            batch_row_keys=args.batch_row_keys,
            value_bytes=args.value_bytes,
        )
        if args.projection_timing == "after":
            create_projection(session, bucket_name, schema_name, table_name)
        if args.settle_seconds > 0:
            print(f"waiting {args.settle_seconds:.1f}s for projection maintenance")
            time.sleep(args.settle_seconds)

        timings = run_benchmark(session, bucket_name, schema_name, table_name, args)
        print_summary(timings)
    finally:
        if args.keep_table:
            print(f"keeping test table: {bucket_name}/{schema_name}/{table_name}")
        else:
            drop_test_table(session, bucket_name, schema_name, table_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
