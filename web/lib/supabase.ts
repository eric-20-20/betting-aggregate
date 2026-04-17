import "server-only";

import { createClient, SupabaseClient } from "@supabase/supabase-js";

/**
 * Server-side Supabase client using the service role key.
 *
 * IMPORTANT: this must never be imported from a client component. The
 * `server-only` import above will fail the build if it is. The service
 * role key bypasses row-level security — only server-side code may hold
 * it.
 *
 * When SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are absent (local dev,
 * preview deploys without secrets), `getSupabase()` returns null and
 * callers must treat this as "Supabase unavailable" — i.e., read-path
 * code must fall back conservatively (fail closed for access, skip
 * writes for the webhook handler).
 */

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

let _client: SupabaseClient | null | undefined;

export function isSupabaseConfigured(): boolean {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

export function getSupabase(): SupabaseClient | null {
  if (_client !== undefined) return _client;
  if (!isSupabaseConfigured()) {
    _client = null;
    return null;
  }
  _client = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
  return _client;
}
