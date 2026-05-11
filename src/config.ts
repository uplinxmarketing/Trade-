// API base URL.
// Empty string = same origin (wolfbot.tech serves both frontend and API).
// Override via VITE_API_URL env var if pointing at a separate backend.
export const API_BASE =
  (import.meta.env.VITE_API_URL as string | undefined) ?? '';
