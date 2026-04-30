import "server-only";

import { getSupabase } from "./supabase";
import {
  buildAdminOverrideEntitlement,
  classifyMembershipStatus,
  coerceSource,
  deriveSubscriptionEntitlement,
  materializeEffectiveEntitlement,
  type AdminActionSnapshot,
  type AdminActionType,
  type EntitlementRecord,
  type EntitlementSource,
  type SubscriptionSnapshot,
} from "./entitlements-core";

export type { AdminActionType, EntitlementRecord, EntitlementSource };
export { classifyMembershipStatus };

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

interface AdminActionRow {
  id: number;
  whop_user_id: string;
  action: AdminActionType;
  reason: string;
  actor_identifier: string;
  created_at: string;
  entitlement_source_before: string | null;
  entitlement_source_after: string | null;
  had_access_before: boolean | null;
  has_access_after: boolean | null;
  metadata: unknown;
}

export interface AdminActionSummary {
  id: number;
  whopUserId: string;
  action: AdminActionType;
  reason: string;
  actorIdentifier: string;
  createdAt: string;
  entitlementSourceBefore: string | null;
  entitlementSourceAfter: string | null;
  hadAccessBefore: boolean | null;
  hasAccessAfter: boolean | null;
  metadata: unknown;
}

const OWNER_USER_ID = process.env.WHOP_OWNER_USER_ID || "";

function mapEntitlementRow(row: EntitlementRow): EntitlementRecord {
  return materializeEffectiveEntitlement({
    whopUserId: row.whop_user_id,
    ownerUserId: OWNER_USER_ID,
    stored: {
      whopUserId: row.whop_user_id,
      hasAccess: Boolean(row.has_access),
      source: coerceSource(row.source),
      expiresAt: row.expires_at,
      lastEventId: row.last_event_id,
      lastUpdatedAt: row.last_updated_at,
    },
  });
}

function mapSubscriptionRow(row: SubscriptionRow): SubscriptionSnapshot {
  return {
    whopMembershipId: row.whop_membership_id,
    whopUserId: row.whop_user_id,
    whopProductId: row.whop_product_id,
    status: row.status,
    isActive: Boolean(row.is_active),
    currentPeriodStart: row.current_period_start,
    currentPeriodEnd: row.current_period_end,
    canceledAt: row.canceled_at,
    lastEventId: row.last_event_id,
    lastEventAt: row.last_event_at,
  };
}

export async function readEntitlement(whopUserId: string): Promise<EntitlementRecord> {
  if (OWNER_USER_ID && whopUserId === OWNER_USER_ID) {
    return materializeEffectiveEntitlement({ whopUserId, ownerUserId: OWNER_USER_ID });
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
    return {
      whopUserId,
      hasAccess: false,
      source: "none",
      expiresAt: null,
      lastEventId: null,
      lastUpdatedAt: null,
    };
  }

  return mapEntitlementRow(data);
}

export interface WebhookPersistInput {
  eventId: string;
  eventType: string;
  signatureVerified: boolean;
  whopUserId: string | null;
  whopMembershipId: string | null;
  rawPayload: unknown;
}

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

async function listSubscriptionsForUser(whopUserId: string): Promise<SubscriptionSnapshot[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("subscriptions")
    .select(
      "whop_membership_id, whop_user_id, whop_product_id, status, is_active, current_period_start, current_period_end, canceled_at, last_event_id, last_event_at"
    )
    .eq("whop_user_id", whopUserId);

  if (error) {
    console.error("[entitlements] listSubscriptionsForUser error:", error);
    throw error;
  }

  return (data || []).map((row) => mapSubscriptionRow(row as SubscriptionRow));
}

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

  const { data: existing, error: existingError } = await supabase
    .from("user_entitlements")
    .select("whop_user_id, has_access, source, expires_at, last_event_id, last_updated_at")
    .eq("whop_user_id", whopUserId)
    .maybeSingle<EntitlementRow>();

  if (existingError) {
    console.error("[entitlements] recompute existing error:", existingError);
    throw existingError;
  }

  const existingSource = coerceSource(existing?.source);
  if (existingSource === "owner" || existingSource === "admin_grant" || existingSource === "admin_revoke") {
    return existing ? mapEntitlementRow(existing) : readEntitlement(whopUserId);
  }

  const subscriptions = await listSubscriptionsForUser(whopUserId);
  const projection = deriveSubscriptionEntitlement(whopUserId, subscriptions, lastEventId);
  const nowIso = new Date().toISOString();

  const { error: upsertErr } = await supabase.from("user_entitlements").upsert(
    {
      whop_user_id: whopUserId,
      has_access: projection.hasAccess,
      source: projection.source,
      expires_at: projection.expiresAt,
      last_event_id: lastEventId,
      last_updated_at: nowIso,
    },
    { onConflict: "whop_user_id" }
  );

  if (upsertErr) {
    console.error("[entitlements] recomputeUserEntitlement upsert error:", upsertErr);
    throw upsertErr;
  }

  return {
    ...projection,
    lastUpdatedAt: nowIso,
  };
}

export async function listAllSubscriptions(): Promise<SubscriptionSnapshot[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("subscriptions")
    .select(
      "whop_membership_id, whop_user_id, whop_product_id, status, is_active, current_period_start, current_period_end, canceled_at, last_event_id, last_event_at"
    )
    .order("last_event_at", { ascending: false });

  if (error) {
    console.error("[entitlements] listAllSubscriptions error:", error);
    return [];
  }

  return (data || []).map((row) => mapSubscriptionRow(row as SubscriptionRow));
}

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

  return (data || []).map((row) => ({
    id: row.id,
    eventType: row.event_type,
    status: row.status,
    signatureVerified: Boolean(row.signature_verified),
    whopUserId: row.whop_user_id,
    whopMembershipId: row.whop_membership_id,
    receivedAt: row.received_at,
    processedAt: row.processed_at,
    processingError: row.processing_error,
  }));
}

export async function listEntitlements(limit = 100): Promise<EntitlementRecord[]> {
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

  return (data || []).map((row) => mapEntitlementRow(row as EntitlementRow));
}

export async function listAllEntitlements(): Promise<EntitlementRecord[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("user_entitlements")
    .select("whop_user_id, has_access, source, expires_at, last_event_id, last_updated_at");

  if (error) {
    console.error("[entitlements] listAllEntitlements error:", error);
    return [];
  }

  return (data || []).map((row) => mapEntitlementRow(row as EntitlementRow));
}

export async function listRecentAdminActions(limit = 100): Promise<AdminActionSummary[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("entitlement_admin_actions")
    .select(
      "id, whop_user_id, action, reason, actor_identifier, created_at, entitlement_source_before, entitlement_source_after, had_access_before, has_access_after, metadata"
    )
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("[entitlements] listRecentAdminActions error:", error);
    return [];
  }

  return (data || []).map((row) => ({
    id: row.id,
    whopUserId: row.whop_user_id,
    action: row.action,
    reason: row.reason,
    actorIdentifier: row.actor_identifier,
    createdAt: row.created_at,
    entitlementSourceBefore: row.entitlement_source_before,
    entitlementSourceAfter: row.entitlement_source_after,
    hadAccessBefore: row.had_access_before,
    hasAccessAfter: row.has_access_after,
    metadata: row.metadata,
  }));
}

export async function applyAdminEntitlementAction(input: {
  whopUserId: string;
  action: AdminActionType;
  reason: string;
  actorIdentifier: string;
  metadata?: Record<string, unknown>;
}): Promise<EntitlementRecord> {
  const supabase = getSupabase();
  if (!supabase) {
    throw new Error("Supabase not configured");
  }

  if (!input.whopUserId.trim()) {
    throw new Error("whopUserId is required");
  }

  if (!input.reason.trim()) {
    throw new Error("reason is required");
  }

  if (OWNER_USER_ID && input.whopUserId === OWNER_USER_ID) {
    throw new Error("Owner entitlement cannot be overridden");
  }

  const before = await readEntitlement(input.whopUserId);
  const nowIso = new Date().toISOString();

  const actionInsert = {
    whop_user_id: input.whopUserId,
    action: input.action,
    reason: input.reason.trim(),
    actor_identifier: input.actorIdentifier,
    entitlement_source_before: before.source,
    entitlement_source_after: input.action === "grant" ? "admin_grant" : "admin_revoke",
    had_access_before: before.hasAccess,
    has_access_after: input.action === "grant",
    metadata: input.metadata ?? {},
  };

  const { data: actionRow, error: actionError } = await supabase
    .from("entitlement_admin_actions")
    .insert(actionInsert)
    .select(
      "id, whop_user_id, action, reason, actor_identifier, created_at, entitlement_source_before, entitlement_source_after, had_access_before, has_access_after, metadata"
    )
    .single<AdminActionRow>();

  if (actionError) {
    console.error("[entitlements] applyAdminEntitlementAction insert error:", actionError);
    throw actionError;
  }

  const next = buildAdminOverrideEntitlement({
    whopUserId: input.whopUserId,
    action: input.action,
    nowIso,
    lastEventId: `admin_action:${actionRow.id}`,
  });

  const { error: entitlementError } = await supabase.from("user_entitlements").upsert(
    {
      whop_user_id: next.whopUserId,
      has_access: next.hasAccess,
      source: next.source,
      expires_at: next.expiresAt,
      last_event_id: next.lastEventId,
      last_updated_at: next.lastUpdatedAt,
    },
    { onConflict: "whop_user_id" }
  );

  if (entitlementError) {
    console.error("[entitlements] applyAdminEntitlementAction upsert error:", entitlementError);
    throw entitlementError;
  }

  return next;
}

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
      cache: "no-store",
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
  for (const membership of memberships) {
    const id = String(membership.id ?? "");
    if (!id) continue;
    await upsertSubscription({
      whopMembershipId: id,
      whopUserId,
      whopProductId: (membership.product_id as string) ?? (membership.product as string) ?? null,
      status: String(membership.status ?? "unknown"),
      currentPeriodStart: (membership.current_period_start as string) ?? null,
      currentPeriodEnd:
        (membership.current_period_end as string) ?? (membership.expires_at as string) ?? null,
      canceledAt: (membership.canceled_at as string) ?? null,
      lastEventId: "admin_refresh",
      lastEventAt: nowIso,
      rawMembership: membership,
    });
  }

  return recomputeUserEntitlement(whopUserId, "admin_refresh");
}

export function toAdminActionSnapshot(action: AdminActionSummary): AdminActionSnapshot {
  return {
    id: action.id,
    whopUserId: action.whopUserId,
    action: action.action,
    reason: action.reason,
    createdAt: action.createdAt,
  };
}
