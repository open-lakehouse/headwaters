#!/usr/bin/env python3
"""Live DuckDB → headwaters lineage journey, via the published duck_lineage extension.

This is the DuckDB sibling of the in-crate DataFusion demo
(`crates/open-lineage/examples/e2e_pipeline`). It runs the *same* bronze → silver
→ gold medallion story, but through a third-party engine integration we don't
control — the DuckDB OpenLineage community extension
(https://github.com/ilum-cloud/duck_lineage) — to prove headwaters ingests and
reconstructs lineage emitted by an external engine over the real HTTP wire path.

Unlike our DataFusion integration (which reads standard OPENLINEAGE_* env vars),
duck_lineage is configured entirely through DuckDB `SET` options. We deliberately
don't modify the extension; we just install it from the community repository and
point it at headwaters.

Run `assert_lineage.py` afterwards to verify the reconstructed graph, or use
`just duck-journey` to do both. Requires a running headwaters (`just dev`).
"""

from __future__ import annotations

import os
import sys

import duckdb

# headwaters' OpenLineage ingest endpoint. Override with OPENLINEAGE_URL.
OPENLINEAGE_URL = os.environ.get(
    "OPENLINEAGE_URL", "http://localhost:8091/api/v1/lineage"
)
# The OpenLineage namespace duck_lineage stamps on jobs. Datasets it emits are
# named by DuckDB's catalog path; we keep the namespace distinct from the
# DataFusion demo's ("datafusion") so the two graphs are easy to tell apart.
NAMESPACE = os.environ.get("DUCK_LINEAGE_NAMESPACE", "duckdb")


def enable_lineage(con: duckdb.DuckDBPyConnection) -> None:
    """Install + load the community extension and point it at headwaters.

    Mirrors the duck_lineage README quickstart. `duck_lineage_debug` echoes each
    emitted event as JSON to stdout, which is handy when eyeballing the run.
    """
    con.execute("INSTALL duck_lineage FROM community")
    con.execute("LOAD duck_lineage")
    con.execute(f"SET duck_lineage_url = '{OPENLINEAGE_URL}'")
    con.execute(f"SET duck_lineage_namespace = '{NAMESPACE}'")
    if os.environ.get("DUCK_LINEAGE_DEBUG"):
        con.execute("SET duck_lineage_debug = true")
    print(f"→ duck_lineage enabled: POST {OPENLINEAGE_URL} (namespace {NAMESPACE})")


def run_pipeline(con: duckdb.DuckDBPyConnection) -> None:
    """The bronze → silver → gold pipeline, matching the DataFusion demo's shape.

    Every statement automatically emits OpenLineage events through the extension's
    optimizer hook — no per-query API. CTAS and INSERT … SELECT both carry their
    output dataset + column lineage.
    """
    print("\n── stage: bronze (seed raw data) ──")
    con.execute(
        """
        CREATE OR REPLACE TABLE raw_orders (
            order_id INT, customer_id INT, amount DOUBLE, country VARCHAR, order_date VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO raw_orders VALUES
            (1, 101, 19.99,  'us', '2026-06-01'),
            (2, 102, 5.50,   'de', '2026-06-01'),
            (3, 101, 120.00, 'us', '2026-06-02'),
            (4, 103, 42.10,  'fr', '2026-06-02'),
            (5, 102, 8.75,   'de', '2026-06-03'),
            (6, 104, 250.00, 'us', '2026-06-03')
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE raw_customers (
            customer_id INT, name VARCHAR, email VARCHAR, home_country VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO raw_customers VALUES
            (101, 'Ada Lovelace', 'ada@example.com',   'US'),
            (102, 'Carl Gauss',   'carl@example.com',  'DE'),
            (103, 'Marie Curie',  'marie@example.com', 'FR'),
            (104, 'Alan Turing',  'alan@example.com',  'US')
        """
    )
    print("  ✓ raw_orders, raw_customers")

    print("── stage: silver (clean + enrich) ──")
    # cast/rename + WHERE filter (TRANSFORMATION + FILTER column lineage).
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_orders AS
        SELECT
            order_id,
            customer_id,
            CAST(amount AS DOUBLE) AS amount_usd,
            upper(country) AS country,
            order_date
        FROM raw_orders
        WHERE amount > 0
        """
    )
    # join orders to customers (JOIN column lineage).
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_orders_enriched AS
        SELECT
            o.order_id,
            o.customer_id,
            c.name AS customer_name,
            o.amount_usd,
            o.country,
            o.order_date
        FROM silver_orders o
        JOIN raw_customers c ON o.customer_id = c.customer_id
        """
    )
    print("  ✓ silver_orders, silver_orders_enriched")

    print("── stage: gold (aggregate) ──")
    # SUM + GROUP BY (AGGREGATION + GROUP_BY column lineage).
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_revenue_by_country AS
        SELECT
            country,
            count(*) AS order_count,
            sum(amount_usd) AS revenue_usd
        FROM silver_orders_enriched
        GROUP BY country
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_daily_orders AS
        SELECT
            order_date,
            count(*) AS order_count,
            sum(amount_usd) AS revenue_usd
        FROM silver_orders_enriched
        GROUP BY order_date
        """
    )
    print("  ✓ gold_revenue_by_country, gold_daily_orders")


def main() -> int:
    con = duckdb.connect()  # in-memory database
    try:
        enable_lineage(con)
    except duckdb.Error as exc:  # pragma: no cover - environment-dependent
        print(f"error: could not enable duck_lineage: {exc}", file=sys.stderr)
        print(
            "  The duck_lineage community extension may not be available for this "
            "DuckDB version/platform. See examples/journeys/duckdb/README.md.",
            file=sys.stderr,
        )
        return 1
    run_pipeline(con)
    # The extension drains its event queue on a background thread; closing the
    # connection flushes outstanding events before the process exits.
    con.close()
    print("\n✓ journey emitted — now run assert_lineage.py (or `just duck-journey`)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
