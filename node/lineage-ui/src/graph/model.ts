// View-model helpers over the lineage graph. The read API delivers each node's
// payload as a `google.protobuf.Struct` (a plain JS object in protobuf-es), so
// the graph treats it as typed-on-read: small, defensive accessors pull out only
// the fields the nodes render, tolerating missing keys.

import type { LineageGraph, LineageNode } from "@headwaters/lineage-client";

/** The node kinds the read API emits in a lineage graph. */
export type LineageNodeKind = "JOB" | "DATASET" | "DATASET_FIELD";

export interface DatasetNodeData {
  namespace: string;
  name: string;
  fieldCount: number;
  tags: string[];
}

export interface JobNodeData {
  namespace: string;
  name: string;
  /** Latest run state (NEW | RUNNING | COMPLETED | FAILED | ABORTED), if any. */
  latestRunState?: string;
  inputCount: number;
  outputCount: number;
}

export interface FieldNodeData {
  /** The owning dataset's display name, when derivable from the field nodeId. */
  dataset: string;
  field: string;
}

function asString(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

/** `{namespace, name}` from a node's `data.id`, falling back to top-level keys. */
function entityId(data: Record<string, unknown> | undefined): {
  namespace: string;
  name: string;
} {
  const id = (data?.id ?? {}) as Record<string, unknown>;
  return {
    namespace: asString(id.namespace) || asString(data?.namespace),
    name: asString(id.name) || asString(data?.name),
  };
}

export function datasetNodeData(node: LineageNode): DatasetNodeData {
  const data = node.data as Record<string, unknown> | undefined;
  const { namespace, name } = entityId(data);
  return {
    namespace,
    name,
    fieldCount: asArray(data?.fields).length,
    tags: asArray(data?.tags).map(asString).filter(Boolean),
  };
}

export function jobNodeData(node: LineageNode): JobNodeData {
  const data = node.data as Record<string, unknown> | undefined;
  const { namespace, name } = entityId(data);
  const latestRun = data?.latestRun as Record<string, unknown> | undefined;
  return {
    namespace,
    name,
    latestRunState: latestRun
      ? asString(latestRun.state) || undefined
      : undefined,
    inputCount: asArray(data?.inputs).length,
    outputCount: asArray(data?.outputs).length,
  };
}

// A field nodeId is `datasetField:<namespace>:<dataset>:<field>`; the dataset
// name may itself contain `:`, so the field is the last segment.
export function fieldNodeData(node: LineageNode): FieldNodeData {
  const rest = node.id.startsWith("datasetField:")
    ? node.id.slice("datasetField:".length)
    : node.id;
  const lastColon = rest.lastIndexOf(":");
  const field = lastColon >= 0 ? rest.slice(lastColon + 1) : rest;
  const beforeField = lastColon >= 0 ? rest.slice(0, lastColon) : "";
  const dataset = beforeField.includes(":")
    ? beforeField.slice(beforeField.indexOf(":") + 1)
    : beforeField;
  return { dataset, field };
}

/** Deduplicated edges from every node's in/out edges, as `{source, target}`. */
export function collectEdges(
  graph: LineageGraph,
): { source: string; target: string }[] {
  const seen = new Set<string>();
  const edges: { source: string; target: string }[] = [];
  for (const node of graph.graph) {
    for (const e of [...node.inEdges, ...node.outEdges]) {
      const key = `${e.origin}->${e.destination}`;
      if (seen.has(key)) continue;
      seen.add(key);
      edges.push({ source: e.origin, target: e.destination });
    }
  }
  return edges;
}
