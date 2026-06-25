import type { LineageNode } from "@headwaters/lineage-client";
import type { NodeProps } from "@xyflow/react";
import { Cog } from "lucide-react";
import { RunStateBadge } from "../../components/ui/RunStateBadge.js";
import { jobNodeData } from "../model.js";
import { BaseNode } from "./BaseNode.js";

/** A job (process) node. */
export function JobNode({ data, selected }: NodeProps) {
  const { node } = data as unknown as { node: LineageNode };
  const d = jobNodeData(node);
  return (
    <BaseNode selected={selected} className="h-[92px] w-[240px]">
      <div className="flex items-center justify-between gap-2 px-3 pt-2">
        <span className="flex items-center gap-2 text-xs font-medium text-violet-600 dark:text-violet-400">
          <Cog className="h-3.5 w-3.5" />
          JOB
        </span>
        {d.latestRunState && <RunStateBadge state={d.latestRunState} />}
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
        <div className="mt-1 text-[11px] text-muted-foreground">
          {d.inputCount} in · {d.outputCount} out
        </div>
      </div>
    </BaseNode>
  );
}
