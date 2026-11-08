// Read-client injection. <ReadClientProvider> puts a ReadClient and a cache
// scope into context, and every data hook reads both from there.
//
// Without a `client`, the provider builds one over the `headwaters` binding of
// the `@open-lakehouse/host` host: a <HostProvider>'s host if there is one
// (Storybook, previews), else the registered host, else the page's own origin.
// A `registerTransport` override wins over the host binding. The desktop shell
// therefore needs nothing beyond `registerHost`, and the standalone app keeps
// its `registerTransport`.
//
// The scope is the host's environment key, so switching environments never
// serves the previous environment's cached queries (see queries.ts).

import { HostContext, useOptionalHost } from "@open-lakehouse/host/react";
import {
  createReadClient,
  type ReadClient,
  transportFor,
} from "@open-lakehouse/lineage-client";
import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useMemo,
  useSyncExternalStore,
} from "react";

/** The scope of the query cache when no host is in effect. */
const NO_HOST_SCOPE = "default";

interface ReadContext {
  client: ReadClient;
  scope: string;
}

const ReadClientContext = createContext<ReadContext | null>(null);

export interface ReadClientProviderProps {
  /**
   * The read client to inject. Defaults to one over the host's `headwaters`
   * binding (or a `registerTransport` override, or the page's own origin).
   */
  client?: ReadClient;
  /**
   * Partitions the query cache. Defaults to the host's environment key, so an
   * environment switch refetches instead of showing the old environment's data.
   */
  scope?: string;
  children: ReactNode;
}

/** The environment key of the host in effect, re-read on every switch. */
function useHostScope(): string {
  const environment = useOptionalHost()?.environment;
  const subscribe = useCallback(
    (onChange: () => void) =>
      environment ? environment.subscribe(onChange) : () => {},
    [environment],
  );
  const key = () => environment?.key ?? NO_HOST_SCOPE;
  return useSyncExternalStore(subscribe, key, key);
}

export function ReadClientProvider({
  client,
  scope,
  children,
}: ReadClientProviderProps) {
  // Only a provided host is captured; the registered one is looked up again on
  // every call, so registering it after mount still takes effect.
  const provided = useContext(HostContext) ?? undefined;
  const hostScope = useHostScope();
  const resolved = useMemo(
    () => client ?? createReadClient(transportFor(provided)),
    [client, provided],
  );
  const effectiveScope = scope ?? hostScope;
  const value = useMemo(
    () => ({ client: resolved, scope: effectiveScope }),
    [resolved, effectiveScope],
  );
  return (
    <ReadClientContext.Provider value={value}>
      {children}
    </ReadClientContext.Provider>
  );
}

function useReadContext(): ReadContext {
  const value = useContext(ReadClientContext);
  if (!value) {
    throw new Error(
      "useReadClient must be used within a <ReadClientProvider>.",
    );
  }
  return value;
}

/** The injected read client. Throws if no provider is mounted. */
export function useReadClient(): ReadClient {
  return useReadContext().client;
}

/** The query-cache scope of the nearest provider. */
export function useReadScope(): string {
  return useReadContext().scope;
}
