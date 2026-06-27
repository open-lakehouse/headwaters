-- Hybrid-CQRS lineage store.
--
-- `events` is the append-only source of truth: ingest writes raw OpenLineage
-- events here and returns immediately. The async projection worker tails it by
-- `seq` and folds it into the normalized read tables below, which the
-- Marquez-compatible read API queries. The read tables can be rebuilt from
-- `events` at any time (truncate + reset the cursor), so they are pure
-- projections — no data lives only there.

-- ---------------------------------------------------------------------------
-- Source of truth: append-only raw event log.
-- ---------------------------------------------------------------------------
CREATE TABLE events (
    -- Monotonic ingestion order; the projection cursor advances over this.
    seq                BIGSERIAL PRIMARY KEY,
    -- "run" | "job" | "dataset".
    event_kind         TEXT NOT NULL,
    -- OpenLineage eventType for run events (START/COMPLETE/FAIL/...); null for
    -- job/dataset events.
    event_type         TEXT,
    event_time         TIMESTAMPTZ,
    producer           TEXT,
    schema_url         TEXT,
    run_id             TEXT,
    job_namespace      TEXT,
    job_name           TEXT,
    dataset_namespace  TEXT,
    dataset_name       TEXT,
    -- The original OpenLineage document, verbatim — every facet (official or
    -- custom) round-trips through here.
    raw                JSONB,
    -- Input/output dataset references, lifted for cheap projection.
    inputs             JSONB,
    outputs            JSONB,
    -- The per-event typed ColumnLineageDatasetFacet payload (when present).
    column_lineage     JSONB
);

CREATE INDEX events_event_time_idx ON events (event_time);
CREATE INDEX events_run_id_idx ON events (run_id);

-- The projector's watermark: the highest `events.seq` already folded in.
CREATE TABLE projection_state (
    name      TEXT PRIMARY KEY,
    last_seq  BIGINT NOT NULL DEFAULT 0
);
INSERT INTO projection_state (name, last_seq) VALUES ('marquez', 0);

-- ---------------------------------------------------------------------------
-- Read model (projections). Rebuildable from `events`.
-- ---------------------------------------------------------------------------
CREATE TABLE namespaces (
    name         TEXT PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    description  TEXT
);

CREATE TABLE jobs (
    namespace    TEXT NOT NULL,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    description  TEXT,
    -- Job tags rendered as `key` / `key:value` strings (Marquez job model).
    tags         JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Input/output dataset EntityIds ({namespace,name}), unioned across events.
    inputs       JSONB NOT NULL DEFAULT '[]'::jsonb,
    outputs      JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Event time at which inputs/outputs and description/tags were last set
    -- (latest-event-wins, so an empty terminal event doesn't erase edges/meta).
    edges_at     TIMESTAMPTZ,
    meta_at      TIMESTAMPTZ,
    PRIMARY KEY (namespace, name)
);

CREATE TABLE runs (
    run_id       TEXT PRIMARY KEY,
    job_namespace TEXT NOT NULL,
    job_name     TEXT NOT NULL,
    -- NEW | RUNNING | COMPLETED | FAILED | ABORTED.
    state        TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    started_at   TIMESTAMPTZ,
    ended_at     TIMESTAMPTZ
);
CREATE INDEX runs_job_idx ON runs (job_namespace, job_name);

CREATE TABLE datasets (
    namespace    TEXT NOT NULL,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    -- Latest schema-facet columns (SchemaDatasetFacet fields), as JSON.
    fields       JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Event time the current `fields` came from (latest-schema-wins).
    schema_at    TIMESTAMPTZ,
    PRIMARY KEY (namespace, name)
);

-- Projected job <-> dataset edges, addressed by Marquez nodeId strings
-- (`job:ns:name`, `dataset:ns:name`). Directed: input dataset -> job, job ->
-- output dataset. The lineage graph is a WITH RECURSIVE walk over this.
CREATE TABLE lineage_edges (
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    PRIMARY KEY (origin, destination)
);
CREATE INDEX lineage_edges_destination_idx ON lineage_edges (destination);
