-- UUIDv7 + audit-timestamp infrastructure, adopted from the unitycatalog-rs
-- Postgres conventions (which in turn adopt dverite/postgres-uuidv7-sql and
-- lakekeeper's trigger helpers). We assign surrogate ids and manage
-- `updated_at` DB-side so the read model carries the same valuable identity /
-- audit fields the Marquez reference implementation exposes, rather than
-- omitting them.

-- --- UUIDv7 generator (millisecond precision) --------------------------------
create or replace function uuidv7(timestamptz DEFAULT clock_timestamp()) RETURNS uuid
AS $$
  select encode(
    set_bit(
      set_bit(
        overlay(uuid_send(gen_random_uuid()) placing
          substring(int8send((extract(epoch from $1)*1000)::bigint) from 3)
          from 1 for 6),
        52, 1),
      53, 1), 'hex')::uuid;
$$ LANGUAGE sql volatile parallel safe;

comment on function uuidv7(timestamptz) is
'Generate a uuid-v7 value with a 48-bit timestamp (millisecond precision) and 74 bits of randomness';

-- --- updated_at trigger helpers ----------------------------------------------
create or replace function set_updated_at() returns trigger as $$
begin
    NEW.updated_at = now();
    return NEW;
end;
$$ language plpgsql;

comment on function set_updated_at() is 'Sets the `updated_at` column to the current timestamp';

create or replace function trigger_updated_at(tablename regclass) returns void as $$
begin
    execute format(
        'CREATE TRIGGER set_updated_at
         BEFORE UPDATE ON %s
         FOR EACH ROW
         WHEN (OLD is distinct from NEW)
         EXECUTE FUNCTION set_updated_at();',
        tablename
    );
end;
$$ language plpgsql;

comment on function trigger_updated_at(regclass) is
'Creates a trigger to set the `updated_at` column to the current timestamp on update';

-- --- surrogate ids + audit timestamps on the read tables ---------------------
-- These are DB-managed: `id` defaults to a fresh UUIDv7, `row_created_at`
-- records first projection, `row_updated_at` is bumped by the trigger on every
-- upsert. They sit alongside the event-derived `created_at` / `updated_at`
-- (which carry the lineage-meaningful timestamps from the event stream).
alter table namespaces
    add column id uuid not null default uuidv7(),
    add column row_created_at timestamptz not null default now(),
    add column row_updated_at timestamptz;
select trigger_updated_at('namespaces');

alter table jobs
    add column id uuid not null default uuidv7(),
    -- `current_version` identifies the job's current input/output + metadata
    -- shape; refreshed by the projector when the edges or metadata change.
    add column current_version uuid not null default uuidv7(),
    add column row_created_at timestamptz not null default now(),
    add column row_updated_at timestamptz;
select trigger_updated_at('jobs');

alter table runs
    add column row_created_at timestamptz not null default now(),
    add column row_updated_at timestamptz;
select trigger_updated_at('runs');

alter table datasets
    add column id uuid not null default uuidv7(),
    -- `current_version` identifies the dataset's current schema; refreshed by
    -- the projector when the schema fields change.
    add column current_version uuid not null default uuidv7(),
    add column row_created_at timestamptz not null default now(),
    add column row_updated_at timestamptz;
select trigger_updated_at('datasets');
