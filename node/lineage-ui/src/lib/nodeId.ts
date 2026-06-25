// Marquez nodeId construction, mirroring the service-side ids.rs. A nodeId
// addresses a graph node as `job:<namespace>:<name>`,
// `dataset:<namespace>:<name>`, or `datasetField:<namespace>:<name>:<field>`.

export function jobNodeId(namespace: string, name: string): string {
  return `job:${namespace}:${name}`;
}

export function datasetNodeId(namespace: string, name: string): string {
  return `dataset:${namespace}:${name}`;
}

/** The kind prefix of a nodeId (`job` | `dataset` | `datasetField`), if any. */
export function nodeIdKind(nodeId: string): string | undefined {
  return nodeId.split(":", 1)[0] || undefined;
}
