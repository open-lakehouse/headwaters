#!/usr/bin/env python3
"""Generate a rich, interconnected OpenLineage dataset for the Headwaters UI.

The goal is a *coherent* demo lineage graph — not random noise — that exercises
every read-API feature the UI surfaces:

  * multiple namespaces (object store, OLTP, warehouse, stream, BI)
  * many jobs across BATCH and STREAMING integrations
  * multiple runs per job with the full START -> COMPLETE / FAIL / ABORT
    lifecycle, nominal-time windows, and parent/child run DAGs
  * dataset schema evolution across runs -> multiple dataset *versions*
  * deep column-lineage chains (4+ hops) with DIRECT / INDIRECT transforms,
    aggregations, joins, and masking
  * PII / governance tags on source columns that propagate downstream through
    column lineage (drives GetTagDownstream)
  * job/dataset/run facets: sourceCodeLocation, sql, jobType, ownership,
    documentation, tags, nominalTime, parent, errorMessage

The output is a single JSON array of OpenLineage events on stdout (or to a file
with -o), ordered so a naive replay reconstructs the graph correctly. The
companion `ingest.sh` POSTs it to a running lineage-service.

Deterministic: no wall-clock, no randomness — re-running produces byte-identical
output, so the seeded DB is reproducible.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PRODUCER = "https://github.com/open-lakehouse/headwaters/examples/seed"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"

# A fixed namespace so generated runIds are stable across regenerations.
_RUN_NS = uuid.UUID("6f3d2c1a-0000-4000-8000-000000000001")


def facet_url(name: str, ver: str) -> str:
    return f"https://openlineage.io/spec/facets/{ver}/{name}.json"


# --------------------------------------------------------------------------- #
# Namespaces — the storage/system boundaries the UI groups assets under.
# --------------------------------------------------------------------------- #
NS_RAW = "kafka://events.prod"  # streaming source
NS_LAKE = "s3://acme-datalake"  # bronze/silver object store
NS_WH = "postgres://warehouse.prod"  # OLTP operational store
NS_ANALYTICS = "snowflake://analytics"  # gold marts
NS_BI = "bigquery://reporting"  # BI extracts


# --------------------------------------------------------------------------- #
# Small builders for facets
# --------------------------------------------------------------------------- #
def base(name: str, ver: str) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": facet_url(name, ver)}


def schema_facet(fields: list[dict]) -> dict:
    """fields: list of {name, type, [description], [tags:[str]]}."""
    out_fields = []
    for f in fields:
        ff = {"name": f["name"], "type": f["type"]}
        if f.get("description"):
            ff["description"] = f["description"]
        if f.get("tags"):
            ff["tags"] = [{"key": t} for t in f["tags"]]
        out_fields.append(ff)
    return {**base("SchemaDatasetFacet", "1-1-1"), "fields": out_fields}


def data_source_facet(name: str, uri: str) -> dict:
    return {**base("DatasourceDatasetFacet", "1-0-0"), "name": name, "uri": uri}


def dataset_doc_facet(description: str) -> dict:
    return {**base("DocumentationDatasetFacet", "1-0-0"), "description": description}


def dataset_tags_facet(tags: list[tuple[str, str, str]]) -> dict:
    """tags: list of (key, value, source)."""
    return {
        **base("TagsDatasetFacet", "1-0-0"),
        "tags": [{"key": k, "value": v, "source": s} for (k, v, s) in tags],
    }


def data_quality_facet(assertions: list[tuple[str, bool, str]]) -> dict:
    """assertions: list of (assertion, success, column)."""
    return {
        **base("DataQualityAssertionsDatasetFacet", "1-0-0"),
        "assertions": [
            {"assertion": a, "success": s, "column": c} for (a, s, c) in assertions
        ],
    }


def column_lineage_facet(fields: dict, dataset_deps: list[dict] | None = None) -> dict:
    cl = {"fields": fields}
    if dataset_deps:
        cl["dataset"] = dataset_deps
    return cl


def in_field(ns: str, name: str, field_name: str, transforms: list[dict]) -> dict:
    return {
        "namespace": ns,
        "name": name,
        "field": field_name,
        "transformations": transforms,
    }


def xform(t: str, sub: str, desc: str = "", masking: bool = False) -> dict:
    return {"type": t, "subtype": sub, "description": desc, "masking": masking}


def job_type_facet(processing: str, integration: str, job_type: str) -> dict:
    return {
        **base("JobTypeJobFacet", "2-0-3"),
        "processingType": processing,
        "integration": integration,
        "jobType": job_type,
    }


def sql_facet(query: str) -> dict:
    return {**base("SQLJobFacet", "1-0-0"), "query": query}


def source_code_facet(path: str, tag: str = "v3.4.0") -> dict:
    return {
        **base("SourceCodeLocationJobFacet", "1-0-0"),
        "type": "git",
        "url": f"https://github.com/acme/data-platform/blob/{tag}/{path}",
        "repoUrl": "https://github.com/acme/data-platform.git",
        "path": path,
        "tag": tag,
    }


def job_doc_facet(description: str) -> dict:
    return {**base("DocumentationJobFacet", "1-0-0"), "description": description}


def ownership_facet(owners: list[tuple[str, str]]) -> dict:
    return {
        **base("OwnershipJobFacet", "1-0-0"),
        "owners": [{"name": n, "type": t} for (n, t) in owners],
    }


def job_tags_facet(tags: list[tuple[str, str, str]]) -> dict:
    return {
        **base("TagsJobFacet", "1-0-0"),
        "tags": [{"key": k, "value": v, "source": s} for (k, v, s) in tags],
    }


def nominal_time_facet(start: datetime, end: datetime) -> dict:
    return {
        **base("NominalTimeRunFacet", "1-0-0"),
        "nominalStartTime": iso(start),
        "nominalEndTime": iso(end),
    }


def parent_run_facet(parent_run_id: str, ns: str, name: str) -> dict:
    return {
        **base("ParentRunFacet", "1-1-0"),
        "run": {"runId": parent_run_id},
        "job": {"namespace": ns, "name": name},
    }


def error_facet(message: str, stack: str = "") -> dict:
    return {
        **base("ErrorMessageRunFacet", "1-0-0"),
        "message": message,
        "programmingLanguage": "PYTHON",
        "stackTrace": stack or message,
    }


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def run_id(job: str, n: int) -> str:
    return str(uuid.uuid5(_RUN_NS, f"{job}#{n}"))


# --------------------------------------------------------------------------- #
# Event registry — events are appended in causal order.
# --------------------------------------------------------------------------- #
EVENTS: list[dict] = []
T0 = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


@dataclass
class Job:
    namespace: str
    name: str
    facets: dict = field(default_factory=dict)


def dataset_obj(
    ns,
    name,
    *,
    schema=None,
    source=None,
    doc=None,
    tags=None,
    column_lineage=None,
    data_quality=None,
    output=True,
) -> dict:
    facets: dict = {}
    if schema is not None:
        facets["schema"] = schema
    if source is not None:
        facets["dataSource"] = source
    if doc is not None:
        facets["documentation"] = doc
    if tags is not None:
        facets["tags"] = tags
    if data_quality is not None:
        facets["dataQualityAssertions"] = data_quality
    if column_lineage is not None:
        facets["columnLineage"] = column_lineage
    obj = {"namespace": ns, "name": name, "facets": facets}
    obj["outputFacets" if output else "inputFacets"] = {}
    return obj


def emit_run(
    job: Job,
    run_no: int,
    *,
    event_type: str,
    at: datetime,
    inputs=None,
    outputs=None,
    run_facets=None,
) -> str:
    rid = run_id(job.name, run_no)
    run: dict = {"runId": rid}
    if run_facets:
        run["facets"] = run_facets
    ev = {
        "eventType": event_type,
        "eventTime": iso(at),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": run,
        "job": {"namespace": job.namespace, "name": job.name, "facets": job.facets},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }
    EVENTS.append(ev)
    return rid


def emit_dataset_event(ns, name, *, at, schema=None, tags=None, doc=None) -> None:
    """A standalone DatasetEvent — used for out-of-band fact discovery (e.g. a
    PII scanner asserting a column is sensitive) and static catalog metadata."""
    facets: dict = {}
    if schema is not None:
        facets["schema"] = schema
    if tags is not None:
        facets["tags"] = tags
    if doc is not None:
        facets["documentation"] = doc
    EVENTS.append(
        {
            "eventType": "COMPLETE",  # ignored for dataset events; harmless
            "eventTime": iso(at),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "dataset": {"namespace": ns, "name": name, "facets": facets},
        }
    )


# =========================================================================== #
# THE GRAPH
#
# A retail/e-commerce analytics platform:
#
#   kafka events ──(stream ingest)──> bronze.clickstream
#   postgres OLTP ─(cdc)────────────> bronze.orders, bronze.customers
#        │                                    │
#        └────(dedup/clean)──> silver.customers (PII), silver.orders
#                                     │
#        clickstream ─(sessionize)──> silver.sessions
#                                     │
#   silver.* ──(join/aggregate)──> gold.customer_360, gold.daily_revenue
#                                     │
#   gold.* ──(extract)──> bi.exec_dashboard
# =========================================================================== #

# --- Source datasets (schemas) -------------------------------------------- #
clickstream_raw_schema = schema_facet(
    [
        {"name": "event_id", "type": "STRING", "description": "Unique click event id"},
        {"name": "session_id", "type": "STRING"},
        {"name": "user_id", "type": "STRING", "tags": ["pii"]},
        {"name": "url", "type": "STRING"},
        {"name": "referrer", "type": "STRING"},
        {"name": "ts", "type": "TIMESTAMP"},
    ]
)

orders_oltp_schema = schema_facet(
    [
        {"name": "order_id", "type": "BIGINT", "description": "Primary key"},
        {"name": "customer_id", "type": "BIGINT"},
        {"name": "amount_cents", "type": "BIGINT"},
        {"name": "currency", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ]
)

customers_oltp_schema = schema_facet(
    [
        {"name": "id", "type": "BIGINT", "description": "Primary key"},
        {"name": "email", "type": "STRING", "tags": ["pii"]},
        {"name": "full_name", "type": "STRING", "tags": ["pii"]},
        {"name": "phone", "type": "STRING", "tags": ["pii"]},
        {"name": "country", "type": "STRING"},
        {"name": "signup_ts", "type": "TIMESTAMP"},
    ]
)

# Evolved customer schema (adds a column) — drives a 2nd dataset version later.
customers_oltp_schema_v2 = schema_facet(
    [
        {"name": "id", "type": "BIGINT", "description": "Primary key"},
        {"name": "email", "type": "STRING", "tags": ["pii"]},
        {"name": "full_name", "type": "STRING", "tags": ["pii"]},
        {"name": "phone", "type": "STRING", "tags": ["pii"]},
        {"name": "country", "type": "STRING"},
        {"name": "marketing_opt_in", "type": "BOOLEAN", "description": "GDPR consent"},
        {"name": "signup_ts", "type": "TIMESTAMP"},
    ]
)


# =========================================================================== #
# 1. STREAMING JOB — clickstream ingest (kafka -> bronze), STREAMING jobType
# =========================================================================== #
stream_job = Job(
    NS_RAW,
    "streaming.clickstream_ingest",
    facets={
        "jobType": job_type_facet("STREAMING", "FLINK", "JOB"),
        "documentation": job_doc_facet(
            "Flink job consuming the prod clickstream topic into the bronze lake."
        ),
        "sourceCodeLocation": source_code_facet("flink/clickstream_ingest.scala"),
        "ownership": ownership_facet(
            [("data-streaming", "TEAM"), ("ana@acme.io", "MAINTAINER")]
        ),
        "tags": job_tags_facet(
            [("tier", "1", "platform"), ("realtime", "", "platform")]
        ),
    },
)

clickstream_bronze = lambda: dataset_obj(  # noqa: E731
    NS_LAKE,
    "bronze.clickstream",
    schema=clickstream_raw_schema,
    source=data_source_facet("acme-datalake", "s3://acme-datalake/bronze/clickstream"),
    doc=dataset_doc_facet("Raw clickstream landed from Kafka, append-only."),
    column_lineage=column_lineage_facet(
        {
            "event_id": {
                "inputFields": [
                    in_field(
                        NS_RAW,
                        "topic.clickstream",
                        "event_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "user_id": {
                "inputFields": [
                    in_field(
                        NS_RAW,
                        "topic.clickstream",
                        "user_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "session_id": {
                "inputFields": [
                    in_field(
                        NS_RAW,
                        "topic.clickstream",
                        "session_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
        }
    ),
)

# A few streaming micro-batches: START then COMPLETE, with nominal windows.
for i in range(3):
    at = T0 + timedelta(hours=i)
    nt = nominal_time_facet(at, at + timedelta(hours=1))
    emit_run(
        stream_job,
        i,
        event_type="START",
        at=at,
        outputs=[clickstream_bronze()],
        run_facets={"nominalTime": nt},
    )
    emit_run(
        stream_job,
        i,
        event_type="COMPLETE",
        at=at + timedelta(minutes=58),
        inputs=[
            dataset_obj(
                NS_RAW, "topic.clickstream", schema=clickstream_raw_schema, output=False
            )
        ],
        outputs=[clickstream_bronze()],
        run_facets={"nominalTime": nt},
    )


# =========================================================================== #
# 2. CDC JOBS — postgres OLTP -> bronze (orders, customers). Schema evolves.
# =========================================================================== #
cdc_orders_job = Job(
    NS_WH,
    "cdc.orders_sync",
    facets={
        "jobType": job_type_facet("BATCH", "AIRBYTE", "JOB"),
        "documentation": job_doc_facet("Debezium CDC of the orders table into bronze."),
        "sourceCodeLocation": source_code_facet("cdc/orders_sync.yaml"),
        "ownership": ownership_facet([("data-eng", "TEAM")]),
    },
)
cdc_customers_job = Job(
    NS_WH,
    "cdc.customers_sync",
    facets={
        "jobType": job_type_facet("BATCH", "AIRBYTE", "JOB"),
        "documentation": job_doc_facet(
            "Debezium CDC of the customers table into bronze."
        ),
        "sourceCodeLocation": source_code_facet("cdc/customers_sync.yaml"),
        "ownership": ownership_facet([("data-eng", "TEAM")]),
    },
)

orders_bronze = lambda: dataset_obj(  # noqa: E731
    NS_LAKE,
    "bronze.orders",
    schema=orders_oltp_schema,
    source=data_source_facet("warehouse", NS_WH),
    doc=dataset_doc_facet("CDC mirror of public.orders."),
    column_lineage=column_lineage_facet(
        {
            f: {
                "inputFields": [
                    in_field(NS_WH, "public.orders", f, [xform("DIRECT", "IDENTITY")])
                ]
            }
            for f in [
                "order_id",
                "customer_id",
                "amount_cents",
                "currency",
                "status",
                "created_at",
            ]
        }
    ),
)


def customers_bronze(schema):
    return dataset_obj(
        NS_LAKE,
        "bronze.customers",
        schema=schema,
        source=data_source_facet("warehouse", NS_WH),
        doc=dataset_doc_facet("CDC mirror of public.customers (contains PII)."),
        tags=dataset_tags_facet(
            [("pii", "true", "governance"), ("domain", "customer", "catalog")]
        ),
        column_lineage=column_lineage_facet(
            {
                f["name"]: {
                    "inputFields": [
                        in_field(
                            NS_WH,
                            "public.customers",
                            f["name"],
                            [xform("DIRECT", "IDENTITY")],
                        )
                    ]
                }
                for f in schema["fields"]
            }
        ),
    )


# Two CDC runs of customers: first with v1 schema, second with the evolved v2
# schema -> two dataset versions for bronze.customers.
day = T0
for i in range(4):
    at = day + timedelta(days=i, hours=2)
    nt = nominal_time_facet(day + timedelta(days=i), day + timedelta(days=i + 1))
    # orders
    emit_run(
        cdc_orders_job,
        i,
        event_type="START",
        at=at,
        outputs=[orders_bronze()],
        run_facets={"nominalTime": nt},
    )
    emit_run(
        cdc_orders_job,
        i,
        event_type="COMPLETE",
        at=at + timedelta(minutes=10),
        inputs=[
            dataset_obj(NS_WH, "public.orders", schema=orders_oltp_schema, output=False)
        ],
        outputs=[orders_bronze()],
        run_facets={"nominalTime": nt},
    )
    # customers — schema evolves at i==2
    cust_schema = customers_oltp_schema if i < 2 else customers_oltp_schema_v2
    emit_run(
        cdc_customers_job,
        i,
        event_type="START",
        at=at,
        outputs=[customers_bronze(cust_schema)],
        run_facets={"nominalTime": nt},
    )
    emit_run(
        cdc_customers_job,
        i,
        event_type="COMPLETE",
        at=at + timedelta(minutes=12),
        inputs=[
            dataset_obj(NS_WH, "public.customers", schema=cust_schema, output=False)
        ],
        outputs=[customers_bronze(cust_schema)],
        run_facets={"nominalTime": nt},
    )


# =========================================================================== #
# 3. SILVER — clean/dedup. silver.customers carries PII; masking applied.
# =========================================================================== #
silver_customers_job = Job(
    NS_LAKE,
    "silver.customers_clean",
    facets={
        "jobType": job_type_facet("BATCH", "SPARK", "QUERY"),
        "sql": sql_facet(
            "SELECT id AS customer_id, sha2(email,256) AS email_hash, "
            "country, signup_ts FROM bronze.customers WHERE id IS NOT NULL"
        ),
        "documentation": job_doc_facet(
            "Deduplicates customers and hashes direct identifiers for the silver layer."
        ),
        "sourceCodeLocation": source_code_facet("spark/silver/customers_clean.py"),
        "ownership": ownership_facet(
            [("data-eng", "TEAM"), ("priya@acme.io", "MAINTAINER")]
        ),
        "tags": job_tags_facet([("governance", "pii-masking", "platform")]),
    },
)

silver_customers_schema = schema_facet(
    [
        {
            "name": "customer_id",
            "type": "BIGINT",
            "description": "From bronze.customers.id",
        },
        {
            "name": "email_hash",
            "type": "STRING",
            "description": "SHA-256 of email (masked)",
        },
        {"name": "country", "type": "STRING"},
        {"name": "signup_ts", "type": "TIMESTAMP"},
    ]
)

silver_customers = lambda: dataset_obj(  # noqa: E731
    NS_ANALYTICS,
    "silver.customers",
    schema=silver_customers_schema,
    doc=dataset_doc_facet("Cleaned customer dimension; PII hashed."),
    column_lineage=column_lineage_facet(
        {
            "customer_id": {
                "inputFields": [
                    in_field(
                        NS_LAKE, "bronze.customers", "id", [xform("DIRECT", "IDENTITY")]
                    )
                ]
            },
            # email -> email_hash is a MASKING transform: PII tag should NOT make
            # email_hash "pii" downstream, but the edge still exists for lineage.
            "email_hash": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.customers",
                        "email",
                        [xform("DIRECT", "TRANSFORMATION", "sha2(email,256)", True)],
                    )
                ]
            },
            "country": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.customers",
                        "country",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "signup_ts": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.customers",
                        "signup_ts",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
        }
    ),
)

silver_orders_job = Job(
    NS_LAKE,
    "silver.orders_clean",
    facets={
        "jobType": job_type_facet("BATCH", "SPARK", "QUERY"),
        "sql": sql_facet(
            "SELECT order_id, customer_id, amount_cents/100.0 AS amount, currency, "
            "status, created_at FROM bronze.orders WHERE status <> 'cancelled'"
        ),
        "documentation": job_doc_facet("Normalizes orders, drops cancelled rows."),
        "sourceCodeLocation": source_code_facet("spark/silver/orders_clean.py"),
        "ownership": ownership_facet([("data-eng", "TEAM")]),
    },
)
silver_orders_schema = schema_facet(
    [
        {"name": "order_id", "type": "BIGINT"},
        {"name": "customer_id", "type": "BIGINT"},
        {"name": "amount", "type": "DOUBLE", "description": "Dollars"},
        {"name": "currency", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ]
)
silver_orders = lambda: dataset_obj(  # noqa: E731
    NS_ANALYTICS,
    "silver.orders",
    schema=silver_orders_schema,
    doc=dataset_doc_facet("Cleaned orders fact."),
    data_quality=data_quality_facet(
        [
            ("not_null", True, "order_id"),
            ("not_null", True, "customer_id"),
            ("positive", True, "amount"),
        ]
    ),
    column_lineage=column_lineage_facet(
        {
            "order_id": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "order_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "customer_id": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "customer_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "amount": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "amount_cents",
                        [xform("DIRECT", "TRANSFORMATION", "amount_cents/100")],
                    )
                ]
            },
            "currency": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "currency",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "status": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "status",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "created_at": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.orders",
                        "created_at",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
        },
        # Dataset-level dependency: the WHERE filter on status.
        dataset_deps=[
            in_field(
                NS_LAKE,
                "bronze.orders",
                "status",
                [xform("INDIRECT", "FILTER", "status <> 'cancelled'")],
            )
        ],
    ),
)

silver_sessions_job = Job(
    NS_LAKE,
    "silver.sessionize",
    facets={
        "jobType": job_type_facet("BATCH", "SPARK", "QUERY"),
        "documentation": job_doc_facet(
            "Sessionizes clickstream into per-user sessions."
        ),
        "sourceCodeLocation": source_code_facet("spark/silver/sessionize.py"),
        "ownership": ownership_facet([("data-streaming", "TEAM")]),
    },
)
silver_sessions_schema = schema_facet(
    [
        {"name": "session_id", "type": "STRING"},
        {"name": "user_id", "type": "STRING", "tags": ["pii"]},
        {"name": "page_views", "type": "BIGINT"},
        {"name": "started_at", "type": "TIMESTAMP"},
    ]
)
silver_sessions = lambda: dataset_obj(  # noqa: E731
    NS_ANALYTICS,
    "silver.sessions",
    schema=silver_sessions_schema,
    doc=dataset_doc_facet("Per-user web sessions."),
    column_lineage=column_lineage_facet(
        {
            "session_id": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.clickstream",
                        "session_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "user_id": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.clickstream",
                        "user_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "page_views": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.clickstream",
                        "event_id",
                        [xform("INDIRECT", "AGGREGATION", "count(*)")],
                    )
                ]
            },
            "started_at": {
                "inputFields": [
                    in_field(
                        NS_LAKE,
                        "bronze.clickstream",
                        "ts",
                        [xform("INDIRECT", "AGGREGATION", "min(ts)")],
                    )
                ]
            },
        },
        dataset_deps=[
            in_field(
                NS_LAKE,
                "bronze.clickstream",
                "session_id",
                [xform("INDIRECT", "GROUP_BY", "GROUP BY session_id, user_id")],
            )
        ],
    ),
)

for i in range(3):
    at = day + timedelta(days=i, hours=4)
    nt = nominal_time_facet(day + timedelta(days=i), day + timedelta(days=i + 1))
    for job, out, inp in [
        (
            silver_customers_job,
            silver_customers(),
            dataset_obj(
                NS_LAKE,
                "bronze.customers",
                schema=customers_oltp_schema if i < 2 else customers_oltp_schema_v2,
                output=False,
            ),
        ),
        (
            silver_orders_job,
            silver_orders(),
            dataset_obj(
                NS_LAKE, "bronze.orders", schema=orders_oltp_schema, output=False
            ),
        ),
        (
            silver_sessions_job,
            silver_sessions(),
            dataset_obj(
                NS_LAKE,
                "bronze.clickstream",
                schema=clickstream_raw_schema,
                output=False,
            ),
        ),
    ]:
        parent = parent_run_facet(
            run_id("orchestrator.daily", i), NS_ANALYTICS, "orchestrator.daily"
        )
        emit_run(
            job,
            i,
            event_type="START",
            at=at,
            inputs=[inp],
            outputs=[out],
            run_facets={"nominalTime": nt, "parent": parent},
        )
        emit_run(
            job,
            i,
            event_type="COMPLETE",
            at=at + timedelta(minutes=20),
            inputs=[inp],
            outputs=[out],
            run_facets={"nominalTime": nt, "parent": parent},
        )


# =========================================================================== #
# 4. GOLD — joins/aggregations. Deep column lineage + a FAILED + an ABORTED run.
# =========================================================================== #
customer_360_job = Job(
    NS_ANALYTICS,
    "gold.customer_360",
    facets={
        "jobType": job_type_facet("BATCH", "DBT", "MODEL"),
        "sql": sql_facet(
            "SELECT c.customer_id, c.country, c.email_hash, "
            "COUNT(o.order_id) AS lifetime_orders, SUM(o.amount) AS lifetime_value, "
            "COALESCE(s.total_sessions,0) AS total_sessions "
            "FROM silver.customers c "
            "LEFT JOIN silver.orders o ON o.customer_id = c.customer_id "
            "LEFT JOIN (SELECT user_id, COUNT(*) total_sessions FROM silver.sessions GROUP BY 1) s "
            "ON s.user_id = CAST(c.customer_id AS STRING) "
            "GROUP BY 1,2,3,6"
        ),
        "documentation": job_doc_facet(
            "The customer 360 mart: identity + behavior + value."
        ),
        "sourceCodeLocation": source_code_facet("dbt/models/gold/customer_360.sql"),
        "ownership": ownership_facet(
            [("analytics-eng", "TEAM"), ("sam@acme.io", "MAINTAINER")]
        ),
        "tags": job_tags_facet(
            [("certified", "", "catalog"), ("domain", "customer", "catalog")]
        ),
    },
)
customer_360_schema = schema_facet(
    [
        {"name": "customer_id", "type": "BIGINT"},
        {"name": "country", "type": "STRING"},
        {"name": "email_hash", "type": "STRING"},
        {"name": "lifetime_orders", "type": "BIGINT"},
        {"name": "lifetime_value", "type": "DOUBLE"},
        {"name": "total_sessions", "type": "BIGINT"},
    ]
)
customer_360 = lambda: dataset_obj(  # noqa: E731
    NS_ANALYTICS,
    "gold.customer_360",
    schema=customer_360_schema,
    doc=dataset_doc_facet("One row per customer with lifetime metrics."),
    tags=dataset_tags_facet(
        [("certified", "gold", "catalog"), ("domain", "customer", "catalog")]
    ),
    data_quality=data_quality_facet(
        [
            ("not_null", True, "customer_id"),
            ("unique", True, "customer_id"),
            ("non_negative", False, "lifetime_value"),  # a failing assertion
        ]
    ),
    column_lineage=column_lineage_facet(
        {
            "customer_id": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.customers",
                        "customer_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "country": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.customers",
                        "country",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "email_hash": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.customers",
                        "email_hash",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "lifetime_orders": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "order_id",
                        [xform("INDIRECT", "AGGREGATION", "count(order_id)")],
                    )
                ]
            },
            "lifetime_value": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "amount",
                        [xform("INDIRECT", "AGGREGATION", "sum(amount)")],
                    )
                ]
            },
            "total_sessions": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.sessions",
                        "session_id",
                        [xform("INDIRECT", "AGGREGATION", "count(*)")],
                    )
                ]
            },
        },
        dataset_deps=[
            in_field(
                NS_ANALYTICS,
                "silver.orders",
                "customer_id",
                [xform("INDIRECT", "JOIN", "o.customer_id = c.customer_id")],
            ),
            in_field(
                NS_ANALYTICS,
                "silver.sessions",
                "user_id",
                [
                    xform(
                        "INDIRECT", "JOIN", "s.user_id = cast(c.customer_id as string)"
                    )
                ],
            ),
        ],
    ),
)

daily_revenue_job = Job(
    NS_ANALYTICS,
    "gold.daily_revenue",
    facets={
        "jobType": job_type_facet("BATCH", "DBT", "MODEL"),
        "sql": sql_facet(
            "SELECT date_trunc('day', created_at) AS day, currency, "
            "SUM(amount) AS revenue, COUNT(*) AS orders FROM silver.orders GROUP BY 1,2"
        ),
        "documentation": job_doc_facet("Daily revenue rollup by currency."),
        "sourceCodeLocation": source_code_facet("dbt/models/gold/daily_revenue.sql"),
        "ownership": ownership_facet([("analytics-eng", "TEAM")]),
    },
)
daily_revenue_schema = schema_facet(
    [
        {"name": "day", "type": "DATE"},
        {"name": "currency", "type": "STRING"},
        {"name": "revenue", "type": "DOUBLE"},
        {"name": "orders", "type": "BIGINT"},
    ]
)
daily_revenue = lambda: dataset_obj(  # noqa: E731
    NS_ANALYTICS,
    "gold.daily_revenue",
    schema=daily_revenue_schema,
    doc=dataset_doc_facet("Revenue per day per currency."),
    tags=dataset_tags_facet([("certified", "gold", "catalog")]),
    column_lineage=column_lineage_facet(
        {
            "day": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "created_at",
                        [xform("INDIRECT", "GROUP_BY", "date_trunc('day',created_at)")],
                    )
                ]
            },
            "currency": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "currency",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "revenue": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "amount",
                        [xform("INDIRECT", "AGGREGATION", "sum(amount)")],
                    )
                ]
            },
            "orders": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "silver.orders",
                        "order_id",
                        [xform("INDIRECT", "AGGREGATION", "count(*)")],
                    )
                ]
            },
        },
    ),
)

for i in range(3):
    at = day + timedelta(days=i, hours=6)
    nt = nominal_time_facet(day + timedelta(days=i), day + timedelta(days=i + 1))
    parent = parent_run_facet(
        run_id("orchestrator.daily", i), NS_ANALYTICS, "orchestrator.daily"
    )
    # customer_360: one run FAILS (i==1) before succeeding the rest.
    if i == 1:
        emit_run(
            customer_360_job,
            i,
            event_type="START",
            at=at,
            inputs=[
                dataset_obj(NS_ANALYTICS, "silver.customers", output=False),
                dataset_obj(NS_ANALYTICS, "silver.orders", output=False),
            ],
            outputs=[customer_360()],
            run_facets={"nominalTime": nt, "parent": parent},
        )
        emit_run(
            customer_360_job,
            i,
            event_type="FAIL",
            at=at + timedelta(minutes=8),
            run_facets={
                "nominalTime": nt,
                "parent": parent,
                "errorMessage": error_facet(
                    "OOM: executor lost during shuffle join on silver.orders"
                ),
            },
        )
    else:
        emit_run(
            customer_360_job,
            i,
            event_type="START",
            at=at,
            inputs=[
                dataset_obj(NS_ANALYTICS, "silver.customers", output=False),
                dataset_obj(NS_ANALYTICS, "silver.orders", output=False),
                dataset_obj(NS_ANALYTICS, "silver.sessions", output=False),
            ],
            outputs=[customer_360()],
            run_facets={"nominalTime": nt, "parent": parent},
        )
        emit_run(
            customer_360_job,
            i,
            event_type="COMPLETE",
            at=at + timedelta(minutes=30),
            inputs=[
                dataset_obj(NS_ANALYTICS, "silver.customers", output=False),
                dataset_obj(NS_ANALYTICS, "silver.orders", output=False),
                dataset_obj(NS_ANALYTICS, "silver.sessions", output=False),
            ],
            outputs=[customer_360()],
            run_facets={"nominalTime": nt, "parent": parent},
        )

    # daily_revenue: one run is ABORTED (i==2).
    inp = dataset_obj(
        NS_ANALYTICS, "silver.orders", schema=silver_orders_schema, output=False
    )
    emit_run(
        daily_revenue_job,
        i,
        event_type="START",
        at=at,
        inputs=[inp],
        outputs=[daily_revenue()],
        run_facets={"nominalTime": nt, "parent": parent},
    )
    if i == 2:
        emit_run(
            daily_revenue_job,
            i,
            event_type="ABORT",
            at=at + timedelta(minutes=3),
            run_facets={
                "nominalTime": nt,
                "parent": parent,
                "errorMessage": error_facet(
                    "Manually aborted: upstream silver.orders late"
                ),
            },
        )
    else:
        emit_run(
            daily_revenue_job,
            i,
            event_type="COMPLETE",
            at=at + timedelta(minutes=15),
            inputs=[inp],
            outputs=[daily_revenue()],
            run_facets={"nominalTime": nt, "parent": parent},
        )


# =========================================================================== #
# 5. BI EXTRACT — gold -> BigQuery, terminal leaf of the graph.
# =========================================================================== #
bi_job = Job(
    NS_BI,
    "bi.exec_dashboard_extract",
    facets={
        "jobType": job_type_facet("BATCH", "FIVETRAN", "JOB"),
        "documentation": job_doc_facet(
            "Extracts gold marts into the BI warehouse for dashboards."
        ),
        "sourceCodeLocation": source_code_facet("bi/exec_dashboard_extract.sql"),
        "ownership": ownership_facet([("bi-team", "TEAM")]),
        "tags": job_tags_facet([("dashboard", "executive", "bi")]),
    },
)
exec_dashboard_schema = schema_facet(
    [
        {"name": "customer_id", "type": "INT64"},
        {"name": "country", "type": "STRING"},
        {"name": "lifetime_value", "type": "FLOAT64"},
        {"name": "total_sessions", "type": "INT64"},
    ]
)
exec_dashboard = lambda: dataset_obj(  # noqa: E731
    NS_BI,
    "marts.exec_customer_overview",
    schema=exec_dashboard_schema,
    doc=dataset_doc_facet("Flattened customer overview powering the exec dashboard."),
    column_lineage=column_lineage_facet(
        {
            "customer_id": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "gold.customer_360",
                        "customer_id",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "country": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "gold.customer_360",
                        "country",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "lifetime_value": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "gold.customer_360",
                        "lifetime_value",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
            "total_sessions": {
                "inputFields": [
                    in_field(
                        NS_ANALYTICS,
                        "gold.customer_360",
                        "total_sessions",
                        [xform("DIRECT", "IDENTITY")],
                    )
                ]
            },
        }
    ),
)
for i in range(2):
    at = day + timedelta(days=i, hours=8)
    emit_run(
        bi_job,
        i,
        event_type="START",
        at=at,
        inputs=[dataset_obj(NS_ANALYTICS, "gold.customer_360", output=False)],
        outputs=[exec_dashboard()],
    )
    emit_run(
        bi_job,
        i,
        event_type="COMPLETE",
        at=at + timedelta(minutes=5),
        inputs=[dataset_obj(NS_ANALYTICS, "gold.customer_360", output=False)],
        outputs=[exec_dashboard()],
    )


# =========================================================================== #
# 6. OUT-OF-BAND FACT DISCOVERY — a governance scanner asserts PII tags via a
#    standalone DatasetEvent (no run). Drives the "synthetic fact" path and adds
#    a `pii` tag on bronze.customers.email that propagates downstream.
# =========================================================================== #
emit_dataset_event(
    NS_LAKE,
    "bronze.customers",
    at=day + timedelta(days=4),
    schema=schema_facet(
        [
            {"name": "email", "type": "STRING", "tags": ["pii"]},
            {"name": "full_name", "type": "STRING", "tags": ["pii", "sensitive"]},
            {"name": "phone", "type": "STRING", "tags": ["pii"]},
        ]
    ),
    tags=dataset_tags_facet([("scanned_by", "presidio", "governance")]),
    doc=dataset_doc_facet("PII classification asserted by the governance scanner."),
)


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--output", help="write to file instead of stdout")
    ap.add_argument("--pretty", action="store_true", help="pretty-print JSON")
    args = ap.parse_args()

    payload = json.dumps(EVENTS, indent=2 if args.pretty else None)
    if args.output:
        with open(args.output, "w") as fh:
            fh.write(payload + "\n")
        print(f"wrote {len(EVENTS)} events to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(payload + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
