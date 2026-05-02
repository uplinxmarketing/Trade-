// Railway backend base URL.
// Set VITE_API_URL in Vercel environment variables to your Railway project URL.
// Locally (Vite dev server) leave unset — proxies handled by vite.config.ts.
export const API_BASE = (import.meta.env.VITE_API_URL as string | undefined) ?? '';
