import { X } from "lucide-react";
import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { useJob, useJobRuns } from "../hooks/queries.js";
import { datasetNodeId } from "../lib/nodeId.js";
import { DetailSection, MetaRow } from "./DetailSection.js";
import { RunList } from "./RunList.js";

export interface JobDetailPanelProps {
  namespace: string;
  name: string;
  /** Optional: render a "View lineage" affordance + clickable in/out datasets. */
  onViewLineage?: (nodeId: string) => void;
  /** Optional: render a close button that dismisses the panel. */
  onClose?: () => void;
}

/** A job's metadata, inputs/outputs, and run history (read API `GetJob`). */
export function JobDetailPanel({
  namespace,
  name,
  onViewLineage,
  onClose,
}: JobDetailPanelProps) {
  const { data: job, isLoading, error } = useJob(namespace, name);
  const { data: runs } = useJobRuns(namespace, name);

  return (
    <AsyncBoundary isLoading={isLoading} error={error}>
      {job && (
        <div className="overflow-auto">
          <header className="flex items-start justify-between gap-3 px-5 py-4">
            <div className="min-w-0">
              <h2 className="truncate text-base font-semibold" title={job.name}>
                {job.simpleName || job.name}
              </h2>
              <p
                className="truncate text-sm text-muted-foreground"
                title={job.namespace}
              >
                {job.namespace}
              </p>
            </div>
            {(onViewLineage || onClose) && (
              <div className="flex shrink-0 items-center gap-1.5">
                {onViewLineage && (
                  <button
                    type="button"
                    className="rounded-md border border-border px-2.5 py-1 text-sm hover:bg-muted"
                    onClick={() =>
                      onViewLineage(`job:${job.namespace}:${job.name}`)
                    }
                  >
                    View lineage
                  </button>
                )}
                {onClose && (
                  <button
                    type="button"
                    aria-label="Close panel"
                    title="Close"
                    className="rounded-md p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                    onClick={onClose}
                  >
                    <X className="h-4 w-4" />
                  </button>
                )}
              </div>
            )}
          </header>

          <DetailSection title="Details">
            <MetaRow label="Type" value={job.type} />
            {job.location && <MetaRow label="Location" value={job.location} />}
            <MetaRow label="Updated" value={job.updatedAt} />
            {job.description && (
              <MetaRow label="Description" value={job.description} />
            )}
          </DetailSection>

          <DetailSection title={`Inputs (${job.inputs.length})`}>
            <DatasetRefs refs={job.inputs} onViewLineage={onViewLineage} />
          </DetailSection>

          <DetailSection title={`Outputs (${job.outputs.length})`}>
            <DatasetRefs refs={job.outputs} onViewLineage={onViewLineage} />
          </DetailSection>

          <DetailSection title="Runs">
            <RunList runs={runs?.runs ?? job.latestRuns} />
          </DetailSection>
        </div>
      )}
    </AsyncBoundary>
  );
}

function DatasetRefs({
  refs,
  onViewLineage,
}: {
  refs: { namespace: string; name: string }[];
  onViewLineage?: (nodeId: string) => void;
}) {
  if (refs.length === 0) {
    return <p className="text-sm text-muted-foreground">None.</p>;
  }
  return (
    <ul className="flex flex-col gap-1 text-sm">
      {refs.map((r) => {
        const label = `${r.namespace} / ${r.name}`;
        return (
          <li
            key={`${r.namespace}/${r.name}`}
            className="truncate"
            title={label}
          >
            {onViewLineage ? (
              <button
                type="button"
                className="text-sky-600 hover:underline dark:text-sky-400"
                onClick={() =>
                  onViewLineage(datasetNodeId(r.namespace, r.name))
                }
              >
                {label}
              </button>
            ) : (
              label
            )}
          </li>
        );
      })}
    </ul>
  );
}
