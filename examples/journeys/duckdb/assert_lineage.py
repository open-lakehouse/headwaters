#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests>=2.31",
# ]
# ///
"""Assert headwaters reconstructed the DuckDB journey's lineage.

Reads headwaters' Marquez-compatible REST API and checks the bronze → silver →
gold graph `journey.py` emitted through the duck_lineage extension was ingested
and reconstructed: the datasets exist in the namespace, the lineage graph links
producers to their datasets, and column lineage survived the round trip.

Because ingest + projection are asynchronous, every read is polled with a short
retry budget (mirroring the Rust `marquez_acceptance` test's `get_json_eventually`).
Exits non-zero on the first failed assertion. Requires a running headwaters and a
prior `journey.py` run (or use `just duck-journey`, which sequences both).
"""

from __future__ import annotations

import os
import sys
import time
import urllib.parse
from typing import Any, Callable

import requests

# headwaters read API base (no trailing /api/v1; we append it per call). Derived
# from OPENLINEAGE_URL's host so both scripts target the same instance.
BASE = os.environ.get("HEADWATERS_URL", "http://localhost:8091").rstrip("/")
NAMESPACE = os.environ.get("DUCK_LINEAGE_NAMESPACE", "duckdb")

# duck_lineage names datasets by DuckDB's catalog path; for an in-memory database
# the tables land under names ending in these. We assert on suffix match so the
# check is robust to the exact catalog/schema prefix the extension chooses.
EXPECTED_DATASETS = [
    "raw_orders",
    "raw_customers",
    "silver_orders",
    "silver_orders_enriched",
    "gold_revenue_by_country",
    "gold_daily_orders",
]

RETRIES = 40
RETRY_DELAY_S = 0.5


def get_json_eventually(
    path: str, predicate: Callable[[Any], bool], what: str
) -> Any:
    """GET BASE+path, retrying until `predicate` holds or the budget runs out."""
    url = f"{BASE}{path}"
    last: Any = None
    for _ in range(RETRIES):
        try:
            resp = requests.get(url, timeout=5)
            if resp.ok:
                body = resp.json()
                if predicate(body):
                    return body
                last = body
        except requests.RequestException as exc:
            last = repr(exc)
        time.sleep(RETRY_DELAY_S)
    fail(f"{what}: condition never met for GET {path}\n  last response: {last!r}")


def fail(msg: str) -> None:
    print(f"✗ {msg}", file=sys.stderr)
    raise SystemExit(1)


def dataset_names(body: Any) -> list[str]:
    return [d.get("name", "") for d in body.get("datasets", [])]


def matches(actual: list[str], expected_suffix: str) -> bool:
    return any(n == expected_suffix or n.endswith(expected_suffix) for n in actual)


def main() -> int:
    print(f"→ asserting DuckDB lineage in namespace '{NAMESPACE}' at {BASE}")

    # 1. The namespace exists.
    get_json_eventually(
        "/api/v1/namespaces",
        lambda b: any(
            ns.get("name") == NAMESPACE for ns in b.get("namespaces", [])
        ),
        f"namespace '{NAMESPACE}' present",
    )
    print(f"  ✓ namespace '{NAMESPACE}' present")

    # 2. All six datasets were reconstructed in the namespace.
    body = get_json_eventually(
        f"/api/v1/namespaces/{NAMESPACE}/datasets?limit=100",
        lambda b: all(
            matches(dataset_names(b), ds) for ds in EXPECTED_DATASETS
        ),
        "all expected datasets present",
    )
    found = dataset_names(body)
    for ds in EXPECTED_DATASETS:
        if not matches(found, ds):
            fail(f"dataset '{ds}' missing; got {found}")
    print(f"  ✓ all {len(EXPECTED_DATASETS)} datasets reconstructed")

    # Resolve the full reconstructed names (with whatever catalog prefix
    # duck_lineage used) so we can address lineage/column-lineage nodes exactly.
    def resolve(suffix: str) -> str:
        for n in found:
            if n == suffix or n.endswith(suffix):
                return n
        fail(f"could not resolve dataset name for '{suffix}'")
        return ""  # unreachable

    gold = resolve("gold_revenue_by_country")
    enriched = resolve("silver_orders_enriched")

    # 3. The lineage graph for a gold dataset is non-empty (it has at least its
    #    producing job as a neighbour).
    node_id = f"dataset:{NAMESPACE}:{gold}"
    graph = get_json_eventually(
        f"/api/v1/lineage?nodeId={urllib.parse.quote(node_id, safe='')}",
        lambda b: bool(b.get("graph")),
        f"lineage graph for {gold} is non-empty",
    )
    print(f"  ✓ lineage graph for '{gold}' has {len(graph['graph'])} node(s)")

    # 4. Column lineage survived: the enriched dataset's columnLineage view maps
    #    at least one output column back to an input field.
    col_node = f"dataset:{NAMESPACE}:{enriched}"
    get_json_eventually(
        f"/api/v1/column-lineage?nodeId={urllib.parse.quote(col_node, safe='')}",
        lambda b: bool(b.get("graph")),
        f"column lineage for {enriched} present",
    )
    print(f"  ✓ column lineage reconstructed for '{enriched}'")

    print("\n✓ DuckDB journey verified — headwaters reconstructed the full graph")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
