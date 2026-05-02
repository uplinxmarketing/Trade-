// Railway backend base URL.
// Override via VITE_API_URL env var in Vercel if the domain ever changes.
export const API_BASE =
  (import.meta.env.VITE_API_URL as string | undefined) ??
  'https://trade-production-a519.up.railway.app';
