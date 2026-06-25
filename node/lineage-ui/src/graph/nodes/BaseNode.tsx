import { Handle, Position } from "@xyflow/react";
import type { ReactNode } from "react";
import { cn } from "../../lib/cn.js";

export interface BaseNodeProps {
  selected?: boolean;
  className?: string;
  children: ReactNode;
}

/**
 * Shared chrome for every lineage node: a bordered card with left (target) and
 * right (source) connection handles for the LEFT→RIGHT layout. Node components
 * supply only their content. Click handling lives on the ReactFlow canvas
 * (`onNodeClick`), the idiomatic place for node interactions.
 */
export function BaseNode({ selected, className, children }: BaseNodeProps) {
  return (
    <div
      className={cn(
        "cursor-pointer rounded-lg border bg-card text-card-foreground shadow-sm transition-shadow hover:shadow-md",
        selected ? "border-primary ring-2 ring-primary/30" : "border-border",
        className,
      )}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!border-border !bg-muted-foreground"
      />
      {children}
      <Handle
        type="source"
        position={Position.Right}
        className="!border-border !bg-muted-foreground"
      />
    </div>
  );
}
