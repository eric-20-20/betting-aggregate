import "server-only";

import { getSupabase } from "./supabase";
import {
  detectReconciliationMismatches,
  type ReconciliationItem,
  type WhopMembershipSnapshot,
} from "./entitlements-core";
import { listAllEntitlements, listAllSubscriptions } from "./entitlements";

export interface ReconciliationRunSummary {
  id: number;
  triggerSource: "manual" | "scheduled" | "system";
  status: "running" | "completed" | "failed";
  totalWhopMemberships: number;
  totalSubscriptionRows: number;
  totalEntitlementRows: number;
  mismatchCount: number;
  unresolvedCount: number;
  initiatedBy: string | null;
  startedAt: string;
  finishedAt: string | null;
  errorMessage: string | null;
  summary: Record<string, unknown>;
}

export interface ReconciliationItemSummary {
  id: number;
  runId: number;
  itemKey: string;
  mismatchType: string;
  severity: "info" | "warning" | "critical";
  entityType: "membership" | "user";
  whopUserId: string | null;
  whopMembershipId: string | null;
  expectedAccess: boolean | null;
  actualAccess: boolean | null;
  details: Record<string, unknown>;
  status: "open" | "resolved" | "auto_resolved";
  resolvedAt: string | null;
  resolvedBy: string | null;
  resolutionNote: string | null;
  createdAt: string;
}

interface ReconciliationRunRow {
  id: number;
  trigger_source: "manual" | "scheduled" | "system";
  status: "running" | "completed" | "failed";
  total_whop_memberships: number;
  total_subscription_rows: number;
  total_entitlement_rows: number;
  mismatch_count: number;
  unresolved_count: number;
  initiated_by: string | null;
  started_at: string;
  finished_at: string | null;
  error_message: string | null;
  summary: Record<string, unknown>;
}

interface ReconciliationItemRow {
  id: number;
  run_id: number;
  item_key: string;
  mismatch_type: string;
  severity: "info" | "warning" | "critical";
  entity_type: "membership" | "user";
  whop_user_id: string | null;
  whop_membership_id: string | null;
  expected_access: boolean | null;
  actual_access: boolean | null;
  details: Record<string, unknown>;
  status: "open" | "resolved" | "auto_resolved";
  resolved_at: string | null;
  resolved_by: string | null;
  resolution_note: string | null;
  created_at: string;
}

function mapRun(row: ReconciliationRunRow): ReconciliationRunSummary {
  return {
    id: row.id,
    triggerSource: row.trigger_source,
    status: row.status,
    totalWhopMemberships: row.total_whop_memberships,
    totalSubscriptionRows: row.total_subscription_rows,
    totalEntitlementRows: row.total_entitlement_rows,
    mismatchCount: row.mismatch_count,
    unresolvedCount: row.unresolved_count,
    initiatedBy: row.initiated_by,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    errorMessage: row.error_message,
    summary: row.summary ?? {},
  };
}

function mapItem(row: ReconciliationItemRow): ReconciliationItemSummary {
  return {
    id: row.id,
    runId: row.run_id,
    itemKey: row.item_key,
    mismatchType: row.mismatch_type,
    severity: row.severity,
    entityType: row.entity_type,
    whopUserId: row.whop_user_id,
    whopMembershipId: row.whop_membership_id,
    expectedAccess: row.expected_access,
    actualAccess: row.actual_access,
    details: row.details ?? {},
    status: row.status,
    resolvedAt: row.resolved_at,
    resolvedBy: row.resolved_by,
    resolutionNote: row.resolution_note,
    createdAt: row.created_at,
  };
}

function buildActorIdentifier(input: {
  initiatedBy?: string | null;
  ip?: string | null;
}): string {
  const parts = [input.initiatedBy || "admin", input.ip || "unknown_ip"];
  return parts.join(":");
}

function normalizeWhopMembership(raw: Record<string, unknown>): WhopMembershipSnapshot | null {
  const id = String(raw.id ?? "");
  const userId = String(
    (raw.user_id as string | undefined) ??
      (raw.user as { id?: string } | undefined)?.id ??
      ""
  );

  if (!id || !userId) return null;

  return {
    id,
    userId,
    productId: (raw.product_id as string) ?? null,
    status: String(raw.status ?? "unknown"),
    currentPeriodStart: (raw.current_period_start as string) ?? null,
    currentPeriodEnd: (raw.current_period_end as string) ?? null,
    expiresAt: (raw.expires_at as string) ?? null,
    canceledAt: (raw.canceled_at as string) ?? null,
  };
}

export async function fetchWhopMembershipsForConfiguredProduct(): Promise<WhopMembershipSnapshot[]> {
  const apiKey = process.env.WHOP_API_KEY || "";
  const productId = process.env.WHOP_PRODUCT_ID || "";
  if (!apiKey || !productId) {
    throw new Error("WHOP_API_KEY and WHOP_PRODUCT_ID are required for reconciliation");
  }

  const memberships: WhopMembershipSnapshot[] = [];
  let page = 1;

  while (page <= 50) {
    const url = new URL("https://api.whop.com/api/v1/memberships");
    url.searchParams.set("product_ids", productId);
    url.searchParams.set("page", String(page));
    url.searchParams.set("per_page", "100");

    const response = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${apiKey}` },
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`Whop memberships fetch failed (${response.status})`);
    }

    const body = (await response.json()) as {
      data?: Array<Record<string, unknown>>;
      pagination?: { total_pages?: number };
      total_pages?: number;
    };

    const rows = Array.isArray(body.data) ? body.data : [];
    for (const row of rows) {
      const membership = normalizeWhopMembership(row);
      if (membership) memberships.push(membership);
    }

    const totalPages = Number(body.pagination?.total_pages ?? body.total_pages ?? page);
    if (rows.length < 100 || page >= totalPages) break;
    page += 1;
  }

  return memberships;
}

export async function listRecentReconciliationRuns(limit = 20): Promise<ReconciliationRunSummary[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("entitlement_reconciliation_runs")
    .select(
      "id, trigger_source, status, total_whop_memberships, total_subscription_rows, total_entitlement_rows, mismatch_count, unresolved_count, initiated_by, started_at, finished_at, error_message, summary"
    )
    .order("started_at", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("[reconciliation] listRecentReconciliationRuns error:", error);
    return [];
  }

  return (data || []).map((row) => mapRun(row as ReconciliationRunRow));
}

export async function getLatestReconciliationRun(): Promise<ReconciliationRunSummary | null> {
  const runs = await listRecentReconciliationRuns(1);
  return runs[0] ?? null;
}

export async function listUnresolvedReconciliationItems(limit = 100): Promise<ReconciliationItemSummary[]> {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from("entitlement_reconciliation_items")
    .select(
      "id, run_id, item_key, mismatch_type, severity, entity_type, whop_user_id, whop_membership_id, expected_access, actual_access, details, status, resolved_at, resolved_by, resolution_note, created_at"
    )
    .eq("status", "open")
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("[reconciliation] listUnresolvedReconciliationItems error:", error);
    return [];
  }

  return (data || []).map((row) => mapItem(row as ReconciliationItemRow));
}

async function createRun(input: {
  triggerSource: "manual" | "scheduled" | "system";
  initiatedBy: string | null;
}): Promise<ReconciliationRunSummary> {
  const supabase = getSupabase();
  if (!supabase) throw new Error("Supabase not configured");

  const { data, error } = await supabase
    .from("entitlement_reconciliation_runs")
    .insert({
      trigger_source: input.triggerSource,
      initiated_by: input.initiatedBy,
      status: "running",
      summary: {},
    })
    .select(
      "id, trigger_source, status, total_whop_memberships, total_subscription_rows, total_entitlement_rows, mismatch_count, unresolved_count, initiated_by, started_at, finished_at, error_message, summary"
    )
    .single<ReconciliationRunRow>();

  if (error) throw error;
  return mapRun(data);
}

async function finalizeRun(runId: number, patch: Partial<ReconciliationRunRow>): Promise<void> {
  const supabase = getSupabase();
  if (!supabase) throw new Error("Supabase not configured");

  const { error } = await supabase
    .from("entitlement_reconciliation_runs")
    .update(patch)
    .eq("id", runId);

  if (error) throw error;
}

async function insertItems(runId: number, items: ReconciliationItem[]): Promise<void> {
  const supabase = getSupabase();
  if (!supabase || items.length === 0) return;

  const rows = items.map((item) => ({
    run_id: runId,
    item_key: item.itemKey,
    mismatch_type: item.mismatchType,
    severity: item.severity,
    entity_type: item.entityType,
    whop_user_id: item.whopUserId,
    whop_membership_id: item.whopMembershipId,
    expected_access: item.expectedAccess,
    actual_access: item.actualAccess,
    details: item.details,
    status: "open",
  }));

  const { error } = await supabase
    .from("entitlement_reconciliation_items")
    .insert(rows);

  if (error) throw error;
}

async function autoResolveItems(runId: number, nextKeys: string[]): Promise<number> {
  const supabase = getSupabase();
  if (!supabase) return 0;

  const { data, error } = await supabase
    .from("entitlement_reconciliation_items")
    .select("id, item_key")
    .eq("status", "open");

  if (error) {
    throw error;
  }

  const nextKeySet = new Set(nextKeys);
  const staleIds = (data || [])
    .filter((row) => !nextKeySet.has(row.item_key))
    .map((row) => row.id);

  if (staleIds.length === 0) return 0;

  const { error: updateError } = await supabase
    .from("entitlement_reconciliation_items")
    .update({
      status: "auto_resolved",
      resolved_at: new Date().toISOString(),
      resolved_by: `run:${runId}`,
      resolution_note: "Resolved automatically by a later reconciliation run",
    })
    .in("id", staleIds);

  if (updateError) {
    throw updateError;
  }

  return staleIds.length;
}

export async function runEntitlementReconciliation(input?: {
  triggerSource?: "manual" | "scheduled" | "system";
  initiatedBy?: string | null;
  ip?: string | null;
}): Promise<ReconciliationRunSummary> {
  const triggerSource = input?.triggerSource ?? "system";
  const initiatedBy = buildActorIdentifier({
    initiatedBy: input?.initiatedBy ?? null,
    ip: input?.ip ?? null,
  });

  const run = await createRun({ triggerSource, initiatedBy });

  try {
    const [whopMemberships, subscriptions, entitlements] = await Promise.all([
      fetchWhopMembershipsForConfiguredProduct(),
      listAllSubscriptions(),
      listAllEntitlements(),
    ]);

    const items = detectReconciliationMismatches({
      whopMemberships,
      subscriptions,
      entitlements,
      ownerUserId: process.env.WHOP_OWNER_USER_ID || "",
    });

    const mismatchCounts = items.reduce<Record<string, number>>((acc, item) => {
      acc[item.mismatchType] = (acc[item.mismatchType] || 0) + 1;
      return acc;
    }, {});

    await autoResolveItems(run.id, items.map((item) => item.itemKey));
    await insertItems(run.id, items);

    const summary = {
      mismatchCounts,
      criticalCount: items.filter((item) => item.severity === "critical").length,
      warningCount: items.filter((item) => item.severity === "warning").length,
      infoCount: items.filter((item) => item.severity === "info").length,
    };

    await finalizeRun(run.id, {
      status: "completed",
      total_whop_memberships: whopMemberships.length,
      total_subscription_rows: subscriptions.length,
      total_entitlement_rows: entitlements.length,
      mismatch_count: items.length,
      unresolved_count: items.length,
      finished_at: new Date().toISOString(),
      error_message: null,
      summary,
    });

    return {
      ...run,
      status: "completed",
      totalWhopMemberships: whopMemberships.length,
      totalSubscriptionRows: subscriptions.length,
      totalEntitlementRows: entitlements.length,
      mismatchCount: items.length,
      unresolvedCount: items.length,
      finishedAt: new Date().toISOString(),
      errorMessage: null,
      summary,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await finalizeRun(run.id, {
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: message,
    });
    throw error;
  }
}
