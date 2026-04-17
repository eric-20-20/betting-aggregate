import "server-only";

import { readEntitlement } from "./entitlements";
import { isSupabaseConfigured } from "./supabase";

const WHOP_API_KEY = process.env.WHOP_API_KEY || "";
const WHOP_PRODUCT_ID = process.env.WHOP_PRODUCT_ID || "";
const WHOP_OWNER_USER_ID = process.env.WHOP_OWNER_USER_ID || "";

/**
 * Short-lived in-memory cache for access decisions.
 *
 * Scope and purpose:
 * - Supabase is the authoritative source of entitlement. `readEntitlement`
 *   is already a single-row indexed lookup, so this cache is primarily a
 *   cold-start buffer and a defense against bursts of hot-path reads
 *   inside one serverless instance.
 * - Webhook handler calls `clearAccessCache(whopUserId)` after a
 *   subscription change commits in Supabase, so stale grants expire
 *   immediately on the instances that serve the revalidation request.
 * - Across multiple serverless instances this cache is NOT coherent.
 *   That is an acceptable trade-off because Supabase read is cheap and
 *   each instance will re-populate from the DB on miss.
 */
const CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

interface CacheEntry {
  hasAccess: boolean;
  expiresAt: number;
}
const accessCache = new Map<string, CacheEntry>();

/**
 * Legacy Whop REST check — retained for recovery flows only.
 *
 * This function is NOT part of the normal access-check hot path. It is
 * available for admin "refresh from Whop" actions and as a last-ditch
 * fallback when Supabase is unreachable.
 */
async function whopRestHasAccess(whopUserId: string): Promise<boolean | null> {
  if (!WHOP_API_KEY || !WHOP_PRODUCT_ID) return null;
  try {
    const url = `https://api.whop.com/api/v1/memberships?user_ids=${encodeURIComponent(
      whopUserId
    )}&product_ids=${encodeURIComponent(WHOP_PRODUCT_ID)}&statuses=active`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${WHOP_API_KEY}` },
    });
    if (!res.ok) {
      console.error(`[whop] REST check failed: ${res.status}`);
      return null;
    }
    const data = (await res.json()) as { data?: unknown[] };
    return Array.isArray(data?.data) && data.data.length > 0;
  } catch (err) {
    console.error("[whop] REST check error:", err);
    return null;
  }
}

/**
 * Whether a Whop user has access to the configured product.
 *
 * Resolution order:
 *   1. Owner bypass (WHOP_OWNER_USER_ID).
 *   2. In-memory cache (2 min TTL).
 *   3. Supabase `user_entitlements` via `readEntitlement`.
 *   4. (Recovery only) Whop REST — if Supabase is unconfigured.
 *
 * Supabase is the authoritative source on the normal path. Whop REST is
 * used ONLY when Supabase is not configured (e.g. local dev, preview
 * without secrets). If Supabase is configured but the row is missing,
 * we fail closed to false — the webhook pipeline is responsible for
 * creating entitlement rows.
 */
export async function hasAccess(whopUserId: string): Promise<boolean> {
  if (!whopUserId) return false;

  // Owner bypass before anything else.
  if (WHOP_OWNER_USER_ID && whopUserId === WHOP_OWNER_USER_ID) return true;

  // Hot-path cache.
  const cached = accessCache.get(whopUserId);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.hasAccess;
  }

  let result: boolean;

  if (isSupabaseConfigured()) {
    const ent = await readEntitlement(whopUserId);
    // `unavailable` means Supabase is configured but the read failed;
    // treat as no access (fail closed).
    result = ent.hasAccess;
  } else {
    // Recovery mode: Supabase isn't wired up. Fall back to Whop REST so
    // the app still works in dev/preview environments without DB setup.
    const whopResult = await whopRestHasAccess(whopUserId);
    result = whopResult === true;
  }

  accessCache.set(whopUserId, {
    hasAccess: result,
    expiresAt: Date.now() + CACHE_TTL_MS,
  });
  return result;
}

/**
 * Clear cached access for a specific user. Called by the webhook
 * handler after a verified subscription change.
 */
export function clearAccessCache(whopUserId: string): void {
  accessCache.delete(whopUserId);
}
