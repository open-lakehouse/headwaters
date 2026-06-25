import type { LineageNode } from "@headwaters/lineage-client";
import { useState } from "react";
import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { useColumnLineage, useLineage } from "../hooks/queries.js";
import { cn } from "../lib/cn.js";
import { LineageCanvas } from "./LineageCanvas.js";

export interface LineageViewProps {
  /** The root nodeId to expand lineage from (e.g. `dataset:ns:name`). */
  nodeId: string;
  /** Selected node id (for highlight), controlled by the host. */
  selectedId?: string;
  /** Called when a node is clicked. */
  onSelect?: (node: LineageNode) => void;
}

type Level = "table" | "column";

/**
 * The data-connected lineage view: a table↔column toggle and a depth control
 * over a `LineageCanvas`. The scaffold app mounts this at `/lineage`; Storybook
 * exercises the presentational `LineageCanvas` directly with fixtures.
 */
export function LineageView({
  nodeId,
  selectedId,
  onSelect,
}: LineageViewProps) {
  const [level, setLevel] = useState<Level>("table");
  const [depth, setDepth] = useState(3);

  const table = useLineage(nodeId, depth);
  // Column lineage takes no depth arg; only fetch when that tab is active.
  const column = useColumnLineage(level === "column" ? nodeId : "");
  const active = level === "table" ? table : column;

  const toggle =
    "rounded-md px-3 py-1 text-sm font-medium transition-colors disabled:opacity-50";

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-4 border-b border-border px-4 py-2">
        <div className="flex items-center gap-1 rounded-lg bg-muted p-0.5">
          <button
            type="button"
            className={cn(
              toggle,
              level === "table" && "bg-background shadow-sm",
            )}
            onClick={() => setLevel("table")}
          >
            Table
          </button>
          <button
            type="button"
            className={cn(
              toggle,
              level === "column" && "bg-background shadow-sm",
            )}
            onClick={() => setLevel("column")}
          >
            Column
          </button>
        </div>
        {level === "table" && (
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            Depth
            <input
              type="range"
              min={1}
              max={10}
              value={depth}
              onChange={(e) => setDepth(Number(e.target.value))}
            />
            <span className="w-4 tabular-nums">{depth}</span>
          </label>
        )}
        <span
          className="ml-auto truncate text-xs text-muted-foreground"
          title={nodeId}
        >
          {nodeId}
        </span>
      </div>
      <div className="min-h-0 flex-1">
        <AsyncBoundary
          isLoading={active.isLoading}
          error={active.error}
          isEmpty={(active.data?.graph.length ?? 0) === 0}
          emptyMessage="No lineage for this node yet."
        >
          <LineageCanvas
            graph={active.data}
            selectedId={selectedId}
            onSelect={onSelect}
          />
        </AsyncBoundary>
      </div>
    </div>
  );
}
