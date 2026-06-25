// Read-client injection — the React-level companion to lineage-client's
// transport seam. A host mounts <ReadClientProvider client={…}> at the root and
// every hook reads the client from context via useReadClient(). The scaffold app
// passes a network-backed client; Storybook passes a fixture-backed one; a host
// app (hydrofoil) passes its own. Components never import createReadClient
// directly, so the data source is fully swappable.

import { createReadClient, type ReadClient } from "@headwaters/lineage-client";
import { createContext, type ReactNode, useContext, useMemo } from "react";

const ReadClientContext = createContext<ReadClient | null>(null);

export interface ReadClientProviderProps {
  /** The read client to inject. Defaults to one bound to the registered transport. */
  client?: ReadClient;
  children: ReactNode;
}

export function ReadClientProvider({
  client,
  children,
}: ReadClientProviderProps) {
  // Default to a client over the registered transport (or the network default).
  const value = useMemo(() => client ?? createReadClient(), [client]);
  return (
    <ReadClientContext.Provider value={value}>
      {children}
    </ReadClientContext.Provider>
  );
}

/** The injected read client. Throws if no provider is mounted. */
export function useReadClient(): ReadClient {
  const client = useContext(ReadClientContext);
  if (!client) {
    throw new Error(
      "useReadClient must be used within a <ReadClientProvider>.",
    );
  }
  return client;
}
