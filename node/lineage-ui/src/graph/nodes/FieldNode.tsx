import type { LineageNode } from "@headwaters/lineage-client";
import type { NodeProps } from "@xyflow/react";
import { Columns3 } from "lucide-react";
import { fieldNodeData } from "../model.js";
import { BaseNode } from "./BaseNode.js";

/** A dataset-field node, used in the column-lineage view. */
export function FieldNode({ data, selected }: NodeProps) {
  const { node } = data as unknown as { node: LineageNode };
  const d = fieldNodeData(node);
  return (
    <BaseNode selected={selected} className="w-[200px]">
      <div className="flex items-center gap-2 px-3 py-2">
        <Columns3 className="h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
        <span className="min-w-0">
          <span className="block truncate text-sm font-medium" title={d.field}>
            {d.field}
          </span>
          <span
            className="block truncate text-[11px] text-muted-foreground"
            title={d.dataset}
          >
            {d.dataset}
          </span>
        </span>
      </div>
    </BaseNode>
  );
}
