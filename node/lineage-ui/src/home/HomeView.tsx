import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { useAssetStats, useLineageEventStats } from "../hooks/queries.js";
import { StatsView } from "./StatsView.js";

/** The landing page: activity charts over the read API's stats endpoints. */
export function HomeView() {
  const events = useLineageEventStats("DAY", 30);
  const datasets = useAssetStats("dataset", "DAY", 30);
  const jobs = useAssetStats("job", "DAY", 30);

  return (
    <AsyncBoundary isLoading={events.isLoading} error={events.error}>
      <div className="grid gap-4 p-6 md:grid-cols-2 xl:grid-cols-3">
        <StatsView
          title="Lineage events"
          buckets={events.data?.buckets ?? []}
        />
        <StatsView title="Datasets" buckets={datasets.data?.buckets ?? []} />
        <StatsView title="Jobs" buckets={jobs.data?.buckets ?? []} />
      </div>
    </AsyncBoundary>
  );
}
