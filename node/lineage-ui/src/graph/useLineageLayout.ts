// ELK-driven layout for the lineage graph. Lineage reads upstream → downstream,
// so we lay nodes out left-to-right with ELK's `layered` algorithm (the same
// engine workflows uses for its DAG, tuned here for lineage proportions).
//
// The hook is async — ELK runs off the main work and resolves to positioned
// ReactFlow nodes + edges. Until it resolves it returns the previous layout (or
// empty), so the canvas never flashes unpositioned nodes.

import type { LineageGraph } from "@headwaters/lineage-client";
import type { Edge, Node } from "@xyflow/react";
import ELK, { type ElkNode } from "elkjs/lib/elk.bundled.js";
import { useEffect, useRef, useState } from "react";
import { collectEdges, type LineageNodeKind } from "./model.js";

const elk = new ELK();

// Per-kind node box sizes (must match the rendered node components).
const SIZES: Record<LineageNodeKind, { width: number; height: number }> = {
  DATASET: { width: 240, height: 76 },
  JOB: { width: 240, height: 76 },
  DATASET_FIELD: { width: 200, height: 44 },
};

const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "RIGHT",
  "elk.layered.spacing.nodeNodeBetweenLayers": "120",
  "elk.spacing.nodeNode": "40",
  "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
  "elk.layered.edgeRouting": "SPLINES",
  "elk.layered.spacing.edgeNodeBetweenLayers": "40",
};

export interface LineageFlow {
  nodes: Node[];
  edges: Edge[];
}

/** Compute a positioned ReactFlow graph from a read-API LineageGraph. */
export function useLineageLayout(graph: LineageGraph | undefined): LineageFlow {
  const [flow, setFlow] = useState<LineageFlow>({ nodes: [], edges: [] });
  // A monotonic token so a stale async layout never overwrites a newer one.
  const runRef = useRef(0);

  useEffect(() => {
    const run = ++runRef.current;
    if (!graph || graph.graph.length === 0) {
      setFlow({ nodes: [], edges: [] });
      return;
    }

    const rawEdges = collectEdges(graph);
    const elkGraph: ElkNode = {
      id: "root",
      layoutOptions: LAYOUT_OPTIONS,
      children: graph.graph.map((n) => {
        const size = SIZES[n.type as LineageNodeKind] ?? SIZES.DATASET;
        return { id: n.id, width: size.width, height: size.height };
      }),
      edges: rawEdges.map((e, i) => ({
        id: `e${i}`,
        sources: [e.source],
        targets: [e.target],
      })),
    };

    elk
      .layout(elkGraph)
      .then((laidOut) => {
        if (run !== runRef.current) return; // superseded
        const positioned = new Map<string, { x: number; y: number }>();
        for (const child of laidOut.children ?? []) {
          positioned.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
        }
        const nodes: Node[] = graph.graph.map((n) => ({
          id: n.id,
          type:
            n.type === "JOB"
              ? "job"
              : n.type === "DATASET_FIELD"
                ? "field"
                : "dataset",
          position: positioned.get(n.id) ?? { x: 0, y: 0 },
          data: { node: n },
        }));
        const edges: Edge[] = rawEdges.map((e, i) => ({
          id: `e${i}`,
          source: e.source,
          target: e.target,
          type: "smoothstep",
        }));
        setFlow({ nodes, edges });
      })
      .catch(() => {
        if (run === runRef.current) setFlow({ nodes: [], edges: [] });
      });
  }, [graph]);

  return flow;
}
