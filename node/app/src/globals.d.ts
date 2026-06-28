// Ambient declarations for the scaffold app.

declare global {
  interface Window {
    /**
     * URL prefix the service is served under, injected into index.html by the
     * headwaters server (see crates/headwaters/src/http.rs). Empty string (or
     * absent) means "served at root". Read once on boot in main.tsx to root the
     * router and the RPC transport under the prefix.
     */
    __HEADWATERS_BASE_PATH__?: string;
  }
}

export {};
