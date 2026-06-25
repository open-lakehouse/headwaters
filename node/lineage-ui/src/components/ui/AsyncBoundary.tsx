import type { ReactNode } from "react";

export interface AsyncBoundaryProps {
  isLoading: boolean;
  error: unknown;
  isEmpty?: boolean;
  emptyMessage?: string;
  children: ReactNode;
}

/**
 * Uniform loading / error / empty rendering for a query-backed view. Keeps every
 * browser and panel consistent and makes the corresponding Storybook states
 * trivial to reproduce.
 */
export function AsyncBoundary({
  isLoading,
  error,
  isEmpty = false,
  emptyMessage = "Nothing here yet.",
  children,
}: AsyncBoundaryProps) {
  if (isLoading) {
    return (
      <div className="flex items-center gap-2 p-6 text-sm text-muted-foreground">
        <span className="h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />
        Loading…
      </div>
    );
  }
  if (error) {
    const message = error instanceof Error ? error.message : String(error);
    return (
      <div className="m-4 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300">
        <p className="font-medium">Something went wrong</p>
        <p className="mt-1 font-mono text-xs opacity-80">{message}</p>
      </div>
    );
  }
  if (isEmpty) {
    return (
      <div className="p-6 text-sm text-muted-foreground">{emptyMessage}</div>
    );
  }
  return <>{children}</>;
}
