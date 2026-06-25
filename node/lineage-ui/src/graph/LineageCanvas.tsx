import type { LineageGraph, LineageNode } from "@headwaters/lineage-client";
import {
  Background,
  type ColorMode,
  Controls,
  type Edge,
  MarkerType,
  type Node,
  type NodeTypes,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import { useCallback, useEffect, useMemo } from "react";
import { DatasetNode } from "./nodes/DatasetNode.js";
import { FieldNode } from "./nodes/FieldNode.js";
import { JobNode } from "./nodes/JobNode.js";
import { useLineageLayout } from "./useLineageLayout.js";

const nodeTypes: NodeTypes = {
  dataset: DatasetNode,
  job: JobNode,
  field: FieldNode,
};

export interface LineageCanvasProps {
  /** The lineage graph to render (read API `GetLineage`/`GetColumnLineage`). */
  graph: LineageGraph | undefined;
  /** Currently-selected node id, highlighted in the canvas. */
  selectedId?: string;
  /** Called when a node is clicked. */
  onSelect?: (node: LineageNode) => void;
  /**
   * Drives ReactFlow's built-in canvas/Controls/Background colors. Defaults to
   * `"system"` (follows the OS preference); a host with an explicit light/dark
   * toggle can pass its active theme so the canvas tracks the rest of the app.
   */
  colorMode?: ColorMode;
}

/**
 * Render a lineage graph as a left-to-right DAG. Purely presentational: it takes
 * a graph and renders it, so the scaffold app feeds it live `useLineage` data
 * and Storybook feeds it fixtures — same component, no backend coupling.
 */
export function LineageCanvas(props: LineageCanvasProps) {
  // ReactFlow hooks (useReactFlow) require a provider ancestor, so the actual
  // flow lives in a child mounted *inside* ReactFlowProvider.
  return (
    <ReactFlowProvider>
      <LineageFlow {...props} />
    </ReactFlowProvider>
  );
}

function LineageFlow({
  graph,
  selectedId,
  onSelect,
  colorMode = "system",
}: LineageCanvasProps) {
  const { nodes, edges } = useLineageLayout(graph);
  const { fitView } = useReactFlow();

  // Highlight the selected node (layout depends only on `graph`, so this is cheap).
  const decoratedNodes = useMemo(
    () => nodes.map((n) => ({ ...n, selected: n.id === selectedId })),
    [nodes, selectedId],
  );

  // Edges carry the read direction (upstream → downstream): an arrowhead on the
  // target end plus a gentle animated dash give the static graph a sense of
  // flow. When a node is selected we emphasize its incident edges (in `primary`)
  // and dim the rest — the Databricks-style "show me what connects here" cue.
  const decoratedEdges = useMemo<Edge[]>(
    () =>
      edges.map((e) => {
        const incident =
          !!selectedId && (e.source === selectedId || e.target === selectedId);
        const dimmed = !!selectedId && !incident;
        // Reference the raw HSL-component tokens (authored in the host's base
        // layer, theme-reactive) rather than Tailwind's @theme-inline color
        // vars, which aren't guaranteed to be emitted as global properties.
        const stroke = incident
          ? "hsl(var(--primary))"
          : "hsl(var(--muted-foreground))";
        return {
          ...e,
          animated: true,
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 16,
            height: 16,
            color: stroke,
          },
          style: {
            stroke,
            strokeWidth: incident ? 2 : 1.5,
            opacity: dimmed ? 0.35 : incident ? 1 : 0.85,
            transition: "opacity 150ms ease, stroke 150ms ease",
          },
        };
      }),
    [edges, selectedId],
  );

  // The layout resolves asynchronously (ELK runs off-thread), so nodes arrive
  // *after* ReactFlow's initial mount — and the `fitView` prop only fits once,
  // on that empty first render. Refit imperatively whenever the laid-out node
  // set changes (initial load, depth change, table↔column toggle) so the graph
  // is always framed instead of sitting off-screen. The key is built from the
  // node ids (not the array identity) so this fires on real layout changes, not
  // on selection-only re-renders.
  const nodeKey = nodes.map((n) => n.id).join("|");
  useEffect(() => {
    if (nodeKey === "") return;
    // rAF lets ReactFlow commit the new nodes before we measure + fit.
    const raf = requestAnimationFrame(() => {
      void fitView({ padding: 0.2, duration: 200 });
    });
    return () => cancelAnimationFrame(raf);
  }, [nodeKey, fitView]);

  const handleNodeClick = useCallback(
    (_: unknown, node: Node) => {
      const lineageNode = (node.data as { node?: LineageNode }).node;
      if (lineageNode && onSelect) onSelect(lineageNode);
    },
    [onSelect],
  );

  return (
    <ReactFlow
      nodes={decoratedNodes}
      edges={decoratedEdges}
      nodeTypes={nodeTypes}
      onNodeClick={handleNodeClick}
      colorMode={colorMode}
      fitView
      proOptions={{ hideAttribution: true }}
      minZoom={0.1}
    >
      <Background />
      <Controls showInteractive={false} />
    </ReactFlow>
  );
}
