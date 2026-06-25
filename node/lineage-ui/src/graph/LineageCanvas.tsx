import type { LineageGraph, LineageNode } from "@headwaters/lineage-client";
import {
  Background,
  Controls,
  type Node,
  type NodeTypes,
  ReactFlow,
  ReactFlowProvider,
} from "@xyflow/react";
import { useCallback, useMemo } from "react";
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
}

/**
 * Render a lineage graph as a left-to-right DAG. Purely presentational: it takes
 * a graph and renders it, so the scaffold app feeds it live `useLineage` data
 * and Storybook feeds it fixtures — same component, no backend coupling.
 */
export function LineageCanvas({
  graph,
  selectedId,
  onSelect,
}: LineageCanvasProps) {
  const { nodes, edges } = useLineageLayout(graph);

  // Highlight the selected node (layout depends only on `graph`, so this is cheap).
  const decoratedNodes = useMemo(
    () => nodes.map((n) => ({ ...n, selected: n.id === selectedId })),
    [nodes, selectedId],
  );

  const handleNodeClick = useCallback(
    (_: unknown, node: Node) => {
      const lineageNode = (node.data as { node?: LineageNode }).node;
      if (lineageNode && onSelect) onSelect(lineageNode);
    },
    [onSelect],
  );

  return (
    <ReactFlowProvider>
      <ReactFlow
        nodes={decoratedNodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={handleNodeClick}
        fitView
        proOptions={{ hideAttribution: true }}
        minZoom={0.1}
      >
        <Background />
        <Controls showInteractive={false} />
      </ReactFlow>
    </ReactFlowProvider>
  );
}
