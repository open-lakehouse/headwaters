-- Richer interpreted facet model. All tables here are pure projections of the
-- `events` log (rebuildable via projection::rebuild), populated by the facet
-- processors. The full set is scaffolded now to avoid migration churn across the
-- phased rollout; each phase wires the processor that fills its tables.
--
-- Phase 1 populates: dataset_fields, column_lineage_edges.
-- Phase 2 populates: dataset_versions.
-- Phase 3 populates: sources + the new job/run/dataset columns.
-- Phase 4 populates: tags, tag_assignments.

-- --- Phase 1: per-column schema + column-lineage edges -----------------------

-- One row per dataset column (from the `schema` facet). The `datasets.fields`
-- JSON cache stays as the denormalized view the current read path uses; these
-- rows make columns first-class for joins (column lineage, field tags).
create table dataset_fields (
    namespace text not null,
    dataset   text not null,
    field     text not null,
    type      text,
    description text,
    ordinal   int not null default 0,
    schema_at timestamptz not null,
    primary key (namespace, dataset, field)
);

-- Column-level lineage edges (from the `columnLineage` facet, output datasets
-- only): input field -> output field.
create table column_lineage_edges (
    in_namespace  text not null,
    in_dataset    text not null,
    in_field      text not null,
    out_namespace text not null,
    out_dataset   text not null,
    out_field     text not null,
    transformation jsonb,
    edge_at       timestamptz not null,
    primary key (in_namespace, in_dataset, in_field, out_namespace, out_dataset, out_field)
);
create index column_lineage_out_idx on column_lineage_edges (out_namespace, out_dataset, out_field);
create index column_lineage_in_idx  on column_lineage_edges (in_namespace, in_dataset, in_field);

-- --- Phase 2: historical dataset versions ------------------------------------

-- A version snapshot per distinct schema, keyed to the producing run (Marquez's
-- per-version dataset model). `version` is a deterministic hash of the schema so
-- replay is idempotent (insert ON CONFLICT DO NOTHING).
create table dataset_versions (
    id         uuid not null default uuidv7(),
    namespace  text not null,
    name       text not null,
    version    uuid not null,
    run_id     text,
    fields     jsonb not null,
    created_at timestamptz not null,
    primary key (namespace, name, version)
);
create index dataset_versions_ds_idx on dataset_versions (namespace, name, created_at desc);

-- --- Phase 3: data sources ---------------------------------------------------

-- The `dataSource` facet catalog (name + connection url).
create table sources (
    name           text primary key,
    connection_url text,
    created_at     timestamptz not null default now(),
    updated_at     timestamptz
);
select trigger_updated_at('sources');

-- --- Phase 4: tags catalog + assignments -------------------------------------

create table tags (
    name        text primary key,
    description text,
    created_at  timestamptz not null default now()
);

-- A tag applied to a dataset, a dataset field, or a job. Add-only,
-- assigned_at-guarded (latest-wins). `field` is null unless target_type is
-- 'dataset_field'.
create table tag_assignments (
    tag         text not null,
    target_type text not null,   -- 'dataset' | 'dataset_field' | 'job'
    namespace   text not null,
    name        text not null,
    field       text not null default '',
    assigned_at timestamptz not null,
    primary key (tag, target_type, namespace, name, field)
);
create index tag_assignments_target_idx on tag_assignments (target_type, namespace, name);

-- --- New columns on existing read tables (filled in Phase 3) -----------------

alter table runs
    add column parent_run_id text,
    add column nominal_start timestamptz,
    add column nominal_end   timestamptz,
    add column error_message text;

alter table jobs
    add column parent_namespace text,
    add column parent_name      text,
    add column location         text,
    add column job_type_facet   text,
    add column source_name      text;

alter table datasets
    add column description text,
    add column source_name text,
    add column deleted     boolean not null default false;
