import "server-only";

import { getSupabase } from "./supabase";

/**
 * Entitlement layer.
 *
 * Source of runtime truth is the Supabase `user_entitlements` table.
 * Webhook events update `subscriptions` -> derive `user_entitlements`.
 * Access checks read `user_entitlements`. Whop's REST is only used for
 * admin/recovery flows (see `refreshEntitlementFromWhop`).
 *
 * Fail-closed posture:
 * - If Supabase is unavailable, `readEntitlement` returns
 *   `{ hasAccess: false, source: "unavailable" }`. Callers treat this
 *   as "no access" (the product degrades to free tier).
 * - `user_entitlements.source` defaults to 'none'. A row must be
 *   explicitly flipped to 'subscription' / 'owner' / 'admin_grant' by
 *   the webhook pipeline or a manual admin action.
 */

export type EntitlementSource =
  | "subscription"
  | "owner"
  | "admin_grant"
  | "none"
  | "unavailable";

export interface EntitlementRecord {
  whopUserId: string;
  hasAccess: boolean;
  source: EntitlementSource;
  expiresAt: string | null;
  lastEventId: string | null;
  lastUpdatedAt: string | null;
}

interface SubscriptionRow {
  whop_membership_id: string;
  whop_user_id: string;
  whop_product_id: string | null;
  status: string;
  is_active: boolean;
  current_period_start: string | null;
  current_period_end: string | null;
  canceled_at: string | null;
  last_event_id: string | null;
  last_event_at: string | null;
  raw_membership: unknown;
}

interface EntitlementRow {
  whop_user_id: string;
  has_access: boolean;
  source: EntitlementSource | string;
  expires_at: string | null;
  last_event_id: string | null;
  last_updated_at: string | null;
}

const OWNER_USER_ID = process.env.WHOP_OWNER_USER_ID || "";

// Sources that can actually be stored in user_entitlements.source.
// `unavailable` is a synthetic read-side value, never written.
const STORABLE_SOURCES = ["subscription", "owner", "admin_grant", "none"] as const;

function coerceSource(raw: unknown): EntitlementSource {
  if (typeof raw !== "string") return "none";
  return (STORABLE_SOURCES as readonly string[]).includes(raw)
    ? (raw as EntitlementSource)
    : "none";
}

// ---------------------------------------------------------------------------
// Read path: hot path for `hasAccess()`
// ---------------------------------------------------------------------------

/**
 * Look up the current entitlement for a whop_user_id.
 * Returns a synthetic "unavailable" record when Supabase isn't configured —
 * callers must treat that as no-access.
 */
export async function readEntitlement(whopUserId: string): Promise<EntitlementRecord> {
  // Owner bypass: always granted regardless of DB state. Recorded so admin
  // surfaces see a consistent row.
  if (OWNER_USER_ID && whopUserId === OWNER_USER_ID) {
    return {
      whopUserId,
      hasAccess: true,
      source: "owner",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  const supabase = getSupabase();
  if (!supabase) {
    return {
      whopUserId,
      hasAccess: false,
      source: "unavailable",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  const { data, error } = await supabase
    .from("user_entitlements")
    .select("whop_user_id, has_access, source, expires_at, last_event_id, last_updated_at")
    .eq("whop_user_id", whopUserId)
    .maybeSingle<EntitlementRow>();

  if (error) {
    console.error("[entitlements] read error:", error);
    return {
      whopUserId,
      hasAccess: false,
      source: "unavailable",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  if (!data) {
    // No row means we've never seen this user in the entitlement pipeline.
    // Fail closed: no access until a webhook or admin action creates a row.
    return {
      whopUserId,
      hasAccess: false,
      source: "none",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  // Honor expires_at even if has_access is true (defense in depth).
  let hasAccess = Boolean(data.has_access);
  if (hasAccess && data.expires_at) {
    const expires = Date.parse(data.expires_at);
    if (!Number.isNaN(expires) && expires < Date.now()) {
      hasAccess = false;
    }
  }

  return {
    whopUserId: data.whop_user_id,
    hasAccess,
    source: coerceSource(data.source),
    expiresAt: data.expires_at,
    lastEventId: data.last_event_id,
    lastUpdatedAt: data.last_updated_at,
  };
}

// ---------------------------------------------------------------------------
// Write path: called by the webhook handler after a verified event
// ---------------------------------------------------------------------------

/**
 * Normalize a Whop membership status string into (status, is_active).
 * Treats the values Whop has historically sent in webhooks; anything
 * else is recorded but defaults to inactive.
 */
export function classifyMembershipStatus(raw: string | null | undefined): {
  status: string;
  isActive: boolean;
} {
  const status = (raw || "").toLowerCase();
  const activeStates = new Set(["active", "trialing", "valid", "completed"]);
  // "completed" in Whop means a lifetime membership that's still granting
  // access. "canceled"/"expired"/"past_due"/"failed" do not grant.
  return { status, isActive: activeStates.has(status) };
}

export interface WebhookPersistInput {
  eventId: string;
  eventType: string;
  signatureVerified: boolean;
  whopUserId: string | null;
  whopMembershipId: string | null;
  rawPayload: unknown;
}

/**
 * Insert the raw webhook into `webhook_events`, with ON CONFLICT DO NOTHING
 * so Whop re-delivery of the same event_id is a cheap no-op.
 * Returns `true` if the row was newly inserted, `false` if it was a
 * duplicate (caller should skip processing).
 */
export async function persistWebhookEvent(input: WebhookPersistInput): Promise<boolean> {
  const supabase = getSupabase();
  if (!supabase) return false;

  const { data, error } = await supabase
    .from("webhook_events")
    .insert({
      id: input.eventId,
      provider: "whop",
      event_type: input.eventType,
      signature_verified: input.signatureVerified,
      whop_user_id: input.whopUserId,
      whop_membership_id: input.whopMembershipId,
      status: "received",
      raw_payload: input.rawPayload,
    })
    .select("id")
    .maybeSingle();

  if (error) {
    // 23505 = unique_violation. Duplicate delivery — not an error.
    if ((error as { code?: string }).code === "23505") return false;
    console.error("[entitlements] persistWebhookEvent error:", error);
    throw error;
  }
  return Boolean(data?.id);
}

export async function markWebhookProcessed(eventId: string): Promise<void> {
  const supabase = getSupabase();
  if (!supabase) return;
  const { error } = await supabase
    .from("webhook_events")
    .update({
      status: "processed",
      processed_at: new Date().toISOString(),
      processing_error: null,
    })
    .eq("id", eventId);
  if (error) console.error("[entitlements] markWebhookProcessed error:", error);
}

export async function markWebhookFailed(eventId: string, err: string): Promise<void> {
  const supabase = getSupabase();
  if (!supabase) return;
  const { error } = await supabase
    .from("webhook_events")
    .update({
      status: "failed",
      processed_at: new Date().toISOString(),
      processing_error: err.slice(0, 2000),
    })
    .eq("id", eventId);
  if (error) console.error("[entitlements] markWebhookFailed error:", error);
}

/**
 * Upsert a subscription row from the Whop webhook payload.
 */
export interface SubscriptionUpsertInput {
  whopMembershipId: string;
  whopUserId: string;
  whopProductId: string | null;
  status: string;
  currentPeriodStart: string | null;
  currentPeriodEnd: string | null;
  canceledAt: string | null;
  lastEventId: string;
  lastEventAt: string;
  rawMembership: unknown;
}

export async function upsertSubscription(input: SubscriptionUpsertInput): Promise<void> {
  const supabase = getSupabase();
  if (!supabase) return;
  const { status, isActive } = classifyMembershipStatus(input.status);
  const row: Omit<SubscriptionRow, "raw_membership"> & { raw_membership: unknown } = {
    whop_membership_id: input.whopMembershipId,
    whop_user_id: input.whopUserId,
    whop_product_id: input.whopProductId,
    status,
    is_active: isActive,
    current_period_start: input.currentPeriodStart,
    current_period_end: input.currentPeriodEnd,
    canceled_at: input.canceledAt,
    last_event_id: input.lastEventId,
    last_event_at: input.lastEventAt,
    raw_membership: input.rawMembership,
  };
  const { error } = await supabase
    .from("subscriptions")
    .upsert(row, { onConflict: "whop_membership_id" });
  if (error) {
    console.error("[entitlements] upsertSubscription error:", error);
    throw error;
  }
}

/**
 * Re-derive `user_entitlements` for a single whop_user_id from
 * `subscriptions`. Idempotent — safe to call repeatedly.
 *
 * Algorithm:
 * - Pick the "best" subscription for this user (any active row; tie-break
 *   by latest current_period_end).
 * - If any active sub exists: set has_access=true, source='subscription',
 *   expires_at = current_period_end.
 * - Otherwise: has_access=false, source='none', expires_at=null.
 *
 * Owner / admin_grant overrides are NOT touched here — those source
 * values are preserved if the existing row already has them.
 */
export async function recomputeUserEntitlement(
  whopUserId: string,
  lastEventId: string | null
): Promise<EntitlementRecord> {
  const supabase = getSupabase();
  if (!supabase) {
    return {
      whopUserId,
      hasAccess: false,
      source: "unavailable",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  // Preserve any existing owner/admin_grant source.
  const { data: existing } = await supabase
    .from("user_entitlements")
    .select("source")
    .eq("whop_user_id", whopUserId)
    .maybeSingle<{ source: EntitlementSource | string }>();

  if (existing?.source === "owner" || existing?.source === "admin_grant") {
    // Leave as-is; admin/owner overrides are not subject to sub changes.
    const fresh = await readEntitlement(whopUserId);
    return fresh;
  }

  const { data: subs, error } = await supabase
    .from("subscriptions")
    .select("is_active, current_period_end, last_event_at")
    .eq("whop_user_id", whopUserId);

  if (error) {
    console.error("[entitlements] recomputeUserEntitlement subs error:", error);
    throw error;
  }

  const activeSubs = (subs || []).filter((s) => s.is_active);
  const chosen = activeSubs.sort((a, b) => {
    const ae = a.current_period_end ? Date.parse(a.current_period_end) : 0;
    const be = b.current_period_end ? Date.parse(b.current_period_end) : 0;
    return be - ae;
  })[0];

  const hasAccess = Boolean(chosen);
  const expiresAt = chosen?.current_period_end ?? null;
  const source: EntitlementSource = hasAccess ? "subscription" : "none";

  const { error: upsertErr } = await supabase.from("user_entitlements").upsert(
    {
      whop_user_id: whopUserId,
      has_access: hasAccess,
      source,
      expires_at: expiresAt,
      last_event_id: lastEventId,
      last_updated_at: new Date().toISOString(),
    },
    { onConflict: "whop_user_id" }
  );

  if (upsertErr) {
    console.error("[entitlements] recomputeUserEntitlement upsert error:", upsertErr);
    throw upsertErr;
  }

  return {
    whopUserId,
    hasAccess,
    source,
    expiresAt,
    lastEventId,
    lastUpdatedAt: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Admin surfaces: listings
// ---------------------------------------------------------------------------

export interface WebhookEventSummary {
  id: string;
  eventType: string;
  status: string;
  signatureVerified: boolean;
  whopUserId: string | null;
  whopMembershipId: string | null;
  receivedAt: string;
  processedAt: string | null;
  processingError: string | null;
}

export async function listRecentWebhookEvents(limit = 50): Promise<WebhookEventSummary[]> {
  const supabase = getSupabase();
  if (!supabase) return [];
  const { data, error } = await supabase
    .from("webhook_events")
    .select(
      "id, event_type, status, signature_verified, whop_user_id, whop_membership_id, received_at, processed_at, processing_error"
    )
    .order("received_at", { ascending: false })
    .limit(limit);
  if (error) {
    console.error("[entitlements] listRecentWebhookEvents error:", error);
    return [];
  }
  return (data || []).map((r) => ({
    id: r.id,
    eventType: r.event_type,
    status: r.status,
    signatureVerified: Boolean(r.signature_verified),
    whopUserId: r.whop_user_id,
    whopMembershipId: r.whop_membership_id,
    receivedAt: r.received_at,
    processedAt: r.processed_at,
    processingError: r.processing_error,
  }));
}

export async function listEntitlements(
  limit = 100
): Promise<EntitlementRecord[]> {
  const supabase = getSupabase();
  if (!supabase) return [];
  const { data, error } = await supabase
    .from("user_entitlements")
    .select("whop_user_id, has_access, source, expires_at, last_event_id, last_updated_at")
    .order("last_updated_at", { ascending: false })
    .limit(limit);
  if (error) {
    console.error("[entitlements] listEntitlements error:", error);
    return [];
  }
  return (data || []).map((d) => ({
    whopUserId: d.whop_user_id,
    hasAccess: Boolean(d.has_access),
    source: coerceSource(d.source),
    expiresAt: d.expires_at,
    lastEventId: d.last_event_id,
    lastUpdatedAt: d.last_updated_at,
  }));
}

// ---------------------------------------------------------------------------
// Admin/recovery: Whop API fallback
// ---------------------------------------------------------------------------

/**
 * Directly query Whop for a user's memberships and re-derive the
 * subscription + entitlement rows. NOT used on the hot path — only
 * invoked from admin flows (e.g. "Refresh from Whop") when the DB is
 * suspected to be stale.
 */
export async function refreshEntitlementFromWhop(whopUserId: string): Promise<EntitlementRecord> {
  const apiKey = process.env.WHOP_API_KEY || "";
  const productId = process.env.WHOP_PRODUCT_ID || "";
  if (!apiKey || !productId) {
    return readEntitlement(whopUserId);
  }

  const url = `https://api.whop.com/api/v1/memberships?user_ids=${encodeURIComponent(
    whopUserId
  )}&product_ids=${encodeURIComponent(productId)}`;
  let memberships: Array<Record<string, unknown>> = [];
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) {
      console.error("[entitlements] refresh: whop fetch failed", res.status);
      return readEntitlement(whopUserId);
    }
    const body = (await res.json()) as { data?: Array<Record<string, unknown>> };
    memberships = Array.isArray(body?.data) ? body.data : [];
  } catch (err) {
    console.error("[entitlements] refresh: whop error", err);
    return readEntitlement(whopUserId);
  }

  const nowIso = new Date().toISOString();
  for (const m of memberships) {
    const id = String(m.id ?? "");
    if (!id) continue;
    await upsertSubscription({
      whopMembershipId: id,
      whopUserId,
      whopProductId: (m.product_id as string) ?? (m.product as string) ?? null,
      status: String(m.status ?? "unknown"),
      currentPeriodStart: (m.current_period_start as string) ?? null,
      currentPeriodEnd:
        (m.current_period_end as string) ?? (m.expires_at as string) ?? null,
      canceledAt: (m.canceled_at as string) ?? null,
      lastEventId: "admin_refresh",
      lastEventAt: nowIso,
      rawMembership: m,
    });
  }

  return recomputeUserEntitlement(whopUserId, "admin_refresh");
}
