import { createClient } from '@supabase/supabase-js';
import type { Database } from './types';

// Publishable (anon) keys are public by design — security is via Supabase RLS.
const SUPABASE_URL =
  (import.meta.env.VITE_SUPABASE_URL as string | undefined) ??
  'https://vrmnmedrwsddxovqcuns.supabase.co';
const SUPABASE_PUBLISHABLE_KEY =
  (import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY as string | undefined) ??
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZybW5tZWRyd3NkZHhvdnFjdW5zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc3MjYwNTIsImV4cCI6MjA5MzMwMjA1Mn0.sV_iJu0CYQwQ9aQGWgFfusqH92QhPKgThOWrCmKKXJo';

export const supabaseConfigured = true;

export const supabase = createClient<Database>(
  SUPABASE_URL,
  SUPABASE_PUBLISHABLE_KEY,
  {
    auth: {
      storage: localStorage,
      persistSession: true,
      autoRefreshToken: true,
    },
  }
);
