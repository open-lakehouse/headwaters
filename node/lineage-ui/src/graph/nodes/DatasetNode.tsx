import type { LineageNode } from "@headwaters/lineage-client";
import type { NodeProps } from "@xyflow/react";
import { Database } from "lucide-react";
import { datasetNodeData } from "../model.js";
import { BaseNode } from "./BaseNode.js";

/** A dataset (table) node. */
export function DatasetNode({ data, selected }: NodeProps) {
  const { node } = data as unknown as { node: LineageNode };
  const d = datasetNodeData(node);
  return (
    <BaseNode selected={selected} className="w-[240px]">
      <div className="flex items-center gap-2 px-3 pt-2 text-xs font-medium text-sky-600 dark:text-sky-400">
        <Database className="h-3.5 w-3.5" />
        DATASET
      </div>
      <div className="px-3 pb-2">
        <div className="truncate text-sm font-semibold" title={d.name}>
          {d.name}
        </div>
        <div
          className="truncate text-xs text-muted-foreground"
          title={d.namespace}
        >
          {d.namespace}
        </div>
        <div className="mt-1 flex items-center gap-2 text-[11px] text-muted-foreground">
          {d.fieldCount > 0 && <span>{d.fieldCount} fields</span>}
          {d.tags.length > 0 && <span>· {d.tags.join(", ")}</span>}
        </div>
      </div>
    </BaseNode>
  );
}
