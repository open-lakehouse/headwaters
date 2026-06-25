// Shared mock data for Storybook stories. Shapes mirror the read API's JSON
// (the same camelCase the ConnectRPC client decodes), so stories exercise the
// real component code paths without a backend.

import type {
  Dataset,
  JobDetail,
  LineageGraph,
  RunDetail,
  SearchResult,
  StatBucket,
} from "@headwaters/lineage-client";

export const mockRun: RunDetail = {
  id: "11111111-2222-3333-4444-555555555555",
  state: "COMPLETED",
  createdAt: "2026-04-28T19:30:00+00:00",
  updatedAt: "2026-04-28T19:30:00+00:00",
  nominalStartTime: "2026-04-28T19:00:00+00:00",
  nominalEndTime: "2026-04-28T19:30:00+00:00",
  startedAt: "2026-04-28T19:00:00+00:00",
  endedAt: "2026-04-28T19:30:00+00:00",
  durationMs: 1_800_000n,
} as unknown as RunDetail;

export const mockJob: JobDetail = {
  id: { namespace: "prod-warehouse", name: "etl.customers.hourly" },
  type: "BATCH",
  name: "etl.customers.hourly",
  simpleName: "etl.customers.hourly",
  namespace: "prod-warehouse",
  createdAt: "2026-04-28T19:30:00+00:00",
  updatedAt: "2026-04-28T19:30:00+00:00",
  inputs: [{ namespace: "file:///data/bronze", name: "/customers" }],
  outputs: [{ namespace: "warehouse", name: "silver.customers" }],
  location: "https://github.com/acme/etl/blob/main/customers.py",
  description: "Hourly customer ETL.",
  latestRun: mockRun,
  latestRuns: [mockRun],
  tags: ["tier-1"],
  parentJobName: "",
  parentJobUuid: "",
  currentVersion: "019efed8-726d-7ddc-b478-9dd9519b7243",
} as unknown as JobDetail;

export const mockDataset: Dataset = {
  id: { namespace: "warehouse", name: "silver.customers" },
  type: "DB_TABLE",
  name: "silver.customers",
  physicalName: "silver.customers",
  namespace: "warehouse",
  sourceName: "warehouse",
  createdAt: "2026-04-28T19:30:00+00:00",
  updatedAt: "2026-04-28T19:30:00+00:00",
  description: "Cleaned customer dimension.",
  fields: [
    { name: "customer_id", type: "INTEGER", description: "Surrogate key" },
    { name: "email_hash", type: "VARCHAR", description: "Hashed email (PII)" },
    { name: "created_date", type: "DATE" },
  ],
  facets: {},
  tags: ["pii", "tier-1"],
  deleted: false,
  currentVersion: "019efed8-7273-73b9-a6a6-e7409c11e963",
} as unknown as Dataset;

// A small table-lineage graph: bronze dataset -> ETL job -> silver dataset.
export const mockTableGraph: LineageGraph = {
  graph: [
    {
      id: "dataset:file:///data/bronze:/customers",
      type: "DATASET",
      data: {
        id: { namespace: "file:///data/bronze", name: "/customers" },
        name: "/customers",
        namespace: "file:///data/bronze",
        fields: [{ name: "id" }, { name: "email" }],
      },
      inEdges: [],
      outEdges: [
        {
          origin: "dataset:file:///data/bronze:/customers",
          destination: "job:prod-warehouse:etl.customers.hourly",
        },
      ],
    },
    {
      id: "job:prod-warehouse:etl.customers.hourly",
      type: "JOB",
      data: mockJob as unknown as Record<string, unknown>,
      inEdges: [
        {
          origin: "dataset:file:///data/bronze:/customers",
          destination: "job:prod-warehouse:etl.customers.hourly",
        },
      ],
      outEdges: [
        {
          origin: "job:prod-warehouse:etl.customers.hourly",
          destination: "dataset:warehouse:silver.customers",
        },
      ],
    },
    {
      id: "dataset:warehouse:silver.customers",
      type: "DATASET",
      data: mockDataset as unknown as Record<string, unknown>,
      inEdges: [
        {
          origin: "job:prod-warehouse:etl.customers.hourly",
          destination: "dataset:warehouse:silver.customers",
        },
      ],
      outEdges: [],
    },
  ],
} as unknown as LineageGraph;

// A column-lineage graph: two field-to-field edges
// (id -> customer_id, email -> email_hash).
const edge = (origin: string, destination: string) => ({ origin, destination });
const fieldNode = (rest: string, outEdges: ReturnType<typeof edge>[] = []) => ({
  id: `datasetField:${rest}`,
  type: "DATASET_FIELD",
  data: {},
  inEdges: [],
  outEdges,
});

export const mockColumnGraph: LineageGraph = {
  graph: [
    fieldNode("file:///data/bronze:/customers:id", [
      edge(
        "datasetField:file:///data/bronze:/customers:id",
        "datasetField:warehouse:silver.customers:customer_id",
      ),
    ]),
    fieldNode("file:///data/bronze:/customers:email", [
      edge(
        "datasetField:file:///data/bronze:/customers:email",
        "datasetField:warehouse:silver.customers:email_hash",
      ),
    ]),
    fieldNode("warehouse:silver.customers:customer_id"),
    fieldNode("warehouse:silver.customers:email_hash"),
  ],
} as unknown as LineageGraph;

export const mockSearchResults: SearchResult[] = [
  {
    name: "/customers",
    namespace: "file:///data/bronze",
    nodeId: "dataset:file:///data/bronze:/customers",
    type: "DATASET",
    updatedAt: "2026-04-28T19:30:00+00:00",
  } as unknown as SearchResult,
  {
    name: "etl.customers.hourly",
    namespace: "prod-warehouse",
    nodeId: "job:prod-warehouse:etl.customers.hourly",
    type: "JOB",
    updatedAt: "2026-04-28T19:30:00+00:00",
  } as unknown as SearchResult,
  {
    name: "silver.customers",
    namespace: "warehouse",
    nodeId: "dataset:warehouse:silver.customers",
    type: "DATASET",
    updatedAt: "2026-04-28T19:30:00+00:00",
  } as unknown as SearchResult,
];

// A 30-day stat series with a plausible shape.
export const mockStatBuckets: StatBucket[] = Array.from(
  { length: 30 },
  (_, i) => {
    const day = String(i + 1).padStart(2, "0");
    return {
      date: `2026-05-${day}T00:00:00+00`,
      count: BigInt(Math.round(20 + 15 * Math.sin(i / 3) + (i % 5))),
    } as unknown as StatBucket;
  },
);
