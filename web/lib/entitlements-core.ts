export type EntitlementSource =
  | "subscription"
  | "owner"
  | "admin_grant"
  | "admin_revoke"
  | "none"
  | "unavailable";

export type AdminActionType = "grant" | "revoke";

export interface EntitlementRecord {
  whopUserId: string;
  hasAccess: boolean;
  source: EntitlementSource;
  expiresAt: string | null;
  lastEventId: string | null;
  lastUpdatedAt: string | null;
}

export interface SubscriptionSnapshot {
  whopMembershipId: string;
  whopUserId: string;
  whopProductId: string | null;
  status: string;
  isActive: boolean;
  currentPeriodStart: string | null;
  currentPeriodEnd: string | null;
  canceledAt: string | null;
  lastEventId: string | null;
  lastEventAt: string | null;
}

export interface AdminActionSnapshot {
  id?: string | number;
  whopUserId: string;
  action: AdminActionType;
  reason: string;
  createdAt: string;
}

export interface WhopMembershipSnapshot {
  id: string;
  userId: string;
  productId: string | null;
  status: string;
  currentPeriodStart: string | null;
  currentPeriodEnd: string | null;
  expiresAt: string | null;
  canceledAt: string | null;
}

export interface ReconciliationItem {
  itemKey: string;
  mismatchType: string;
  severity: "info" | "warning" | "critical";
  entityType: "membership" | "user";
  whopUserId: string | null;
  whopMembershipId: string | null;
  expectedAccess: boolean | null;
  actualAccess: boolean | null;
  details: Record<string, unknown>;
}

const ACTIVE_MEMBERSHIP_STATES = new Set(["active", "trialing", "valid", "completed"]);

export const STORABLE_SOURCES = [
  "subscription",
  "owner",
  "admin_grant",
  "admin_revoke",
  "none",
] as const;

export function coerceSource(raw: unknown): EntitlementSource {
  if (typeof raw !== "string") return "none";
  return (STORABLE_SOURCES as readonly string[]).includes(raw)
    ? (raw as EntitlementSource)
    : "none";
}

export function classifyMembershipStatus(raw: string | null | undefined): {
  status: string;
  isActive: boolean;
} {
  const status = (raw || "").toLowerCase();
  return { status, isActive: ACTIVE_MEMBERSHIP_STATES.has(status) };
}

export function chooseBestSubscription(
  subscriptions: SubscriptionSnapshot[]
): SubscriptionSnapshot | null {
  const activeSubs = subscriptions.filter((sub) => sub.isActive);
  if (activeSubs.length === 0) return null;
  return [...activeSubs].sort((a, b) => {
    const aEnd = a.currentPeriodEnd ? Date.parse(a.currentPeriodEnd) : 0;
    const bEnd = b.currentPeriodEnd ? Date.parse(b.currentPeriodEnd) : 0;
    if (bEnd !== aEnd) return bEnd - aEnd;

    const aEvent = a.lastEventAt ? Date.parse(a.lastEventAt) : 0;
    const bEvent = b.lastEventAt ? Date.parse(b.lastEventAt) : 0;
    return bEvent - aEvent;
  })[0];
}

export function deriveSubscriptionEntitlement(
  whopUserId: string,
  subscriptions: SubscriptionSnapshot[],
  lastEventId: string | null
): EntitlementRecord {
  const chosen = chooseBestSubscription(subscriptions);
  return {
    whopUserId,
    hasAccess: Boolean(chosen),
    source: chosen ? "subscription" : "none",
    expiresAt: chosen?.currentPeriodEnd ?? null,
    lastEventId,
    lastUpdatedAt: null,
  };
}

export function materializeEffectiveEntitlement(input: {
  whopUserId: string;
  ownerUserId?: string | null;
  stored?: Partial<EntitlementRecord> | null;
  now?: number;
}): EntitlementRecord {
  const { whopUserId, ownerUserId = "", stored, now = Date.now() } = input;

  if (ownerUserId && whopUserId === ownerUserId) {
    return {
      whopUserId,
      hasAccess: true,
      source: "owner",
      expiresAt: null,
      lastEventId: stored?.lastEventId ?? null,
      lastUpdatedAt: stored?.lastUpdatedAt ?? null,
    };
  }

  const source = coerceSource(stored?.source);
  const expiresAt = stored?.expiresAt ?? null;
  const lastEventId = stored?.lastEventId ?? null;
  const lastUpdatedAt = stored?.lastUpdatedAt ?? null;

  if (source === "admin_grant") {
    return {
      whopUserId,
      hasAccess: true,
      source,
      expiresAt: null,
      lastEventId,
      lastUpdatedAt,
    };
  }

  if (source === "admin_revoke") {
    return {
      whopUserId,
      hasAccess: false,
      source,
      expiresAt: null,
      lastEventId,
      lastUpdatedAt,
    };
  }

  let hasAccess = Boolean(stored?.hasAccess);
  if (hasAccess && expiresAt) {
    const expires = Date.parse(expiresAt);
    if (!Number.isNaN(expires) && expires < now) {
      hasAccess = false;
    }
  }

  return {
    whopUserId,
    hasAccess,
    source,
    expiresAt,
    lastEventId,
    lastUpdatedAt,
  };
}

export function buildAdminOverrideEntitlement(input: {
  whopUserId: string;
  action: AdminActionType;
  nowIso: string;
  lastEventId: string;
}): EntitlementRecord {
  return {
    whopUserId: input.whopUserId,
    hasAccess: input.action === "grant",
    source: input.action === "grant" ? "admin_grant" : "admin_revoke",
    expiresAt: null,
    lastEventId: input.lastEventId,
    lastUpdatedAt: input.nowIso,
  };
}

function normalizeMembership(input: {
  id: string;
  userId: string;
  productId: string | null;
  status: string;
  currentPeriodStart: string | null;
  currentPeriodEnd: string | null;
  expiresAt: string | null;
  canceledAt: string | null;
}): SubscriptionSnapshot {
  const { status, isActive } = classifyMembershipStatus(input.status);
  return {
    whopMembershipId: input.id,
    whopUserId: input.userId,
    whopProductId: input.productId,
    status,
    isActive,
    currentPeriodStart: input.currentPeriodStart,
    currentPeriodEnd: input.currentPeriodEnd ?? input.expiresAt,
    canceledAt: input.canceledAt,
    lastEventId: null,
    lastEventAt: null,
  };
}

function makeItemKey(parts: Array<string | null | undefined>): string {
  return parts.map((part) => part || "none").join("|");
}

export function detectReconciliationMismatches(input: {
  whopMemberships: WhopMembershipSnapshot[];
  subscriptions: SubscriptionSnapshot[];
  entitlements: EntitlementRecord[];
  ownerUserId?: string | null;
}): ReconciliationItem[] {
  const { whopMemberships, subscriptions, entitlements, ownerUserId = "" } = input;
  const items: ReconciliationItem[] = [];

  const remoteByMembershipId = new Map(whopMemberships.map((m) => [m.id, m]));
  const subscriptionByMembershipId = new Map(subscriptions.map((s) => [s.whopMembershipId, s]));
  const entitlementByUserId = new Map(entitlements.map((e) => [e.whopUserId, e]));

  for (const membership of whopMemberships) {
    const remoteProjection = normalizeMembership({
      id: membership.id,
      userId: membership.userId,
      productId: membership.productId,
      status: membership.status,
      currentPeriodStart: membership.currentPeriodStart,
      currentPeriodEnd: membership.currentPeriodEnd,
      expiresAt: membership.expiresAt,
      canceledAt: membership.canceledAt,
    });
    const local = subscriptionByMembershipId.get(membership.id);

    if (!local) {
      items.push({
        itemKey: makeItemKey(["missing_subscription", membership.id]),
        mismatchType: "missing_subscription_for_whop_membership",
        severity: remoteProjection.isActive ? "critical" : "warning",
        entityType: "membership",
        whopUserId: membership.userId,
        whopMembershipId: membership.id,
        expectedAccess: remoteProjection.isActive,
        actualAccess: null,
        details: {
          whopStatus: remoteProjection.status,
          currentPeriodEnd: remoteProjection.currentPeriodEnd,
        },
      });
      continue;
    }

    if (
      local.whopUserId !== remoteProjection.whopUserId ||
      local.status !== remoteProjection.status ||
      local.isActive !== remoteProjection.isActive ||
      (local.currentPeriodEnd ?? null) !== (remoteProjection.currentPeriodEnd ?? null)
    ) {
      items.push({
        itemKey: makeItemKey(["subscription_state", membership.id]),
        mismatchType: "subscription_state_mismatch",
        severity: remoteProjection.isActive ? "critical" : "warning",
        entityType: "membership",
        whopUserId: membership.userId,
        whopMembershipId: membership.id,
        expectedAccess: remoteProjection.isActive,
        actualAccess: local.isActive,
        details: {
          whopUserId: remoteProjection.whopUserId,
          subscriptionUserId: local.whopUserId,
          whopStatus: remoteProjection.status,
          subscriptionStatus: local.status,
          whopCurrentPeriodEnd: remoteProjection.currentPeriodEnd,
          subscriptionCurrentPeriodEnd: local.currentPeriodEnd,
        },
      });
    }
  }

  for (const local of subscriptions) {
    if (remoteByMembershipId.has(local.whopMembershipId)) continue;
    items.push({
      itemKey: makeItemKey(["orphan_subscription", local.whopMembershipId]),
      mismatchType: "subscription_missing_in_whop",
      severity: local.isActive ? "critical" : "warning",
      entityType: "membership",
      whopUserId: local.whopUserId,
      whopMembershipId: local.whopMembershipId,
      expectedAccess: null,
      actualAccess: local.isActive,
      details: {
        subscriptionStatus: local.status,
        subscriptionCurrentPeriodEnd: local.currentPeriodEnd,
      },
    });
  }

  const userIds = new Set<string>();
  for (const membership of whopMemberships) userIds.add(membership.userId);
  for (const subscription of subscriptions) userIds.add(subscription.whopUserId);
  for (const entitlement of entitlements) userIds.add(entitlement.whopUserId);

  for (const userId of userIds) {
    const userSubscriptions = subscriptions.filter((sub) => sub.whopUserId === userId);
    const expected = materializeEffectiveEntitlement({
      whopUserId: userId,
      ownerUserId,
      stored: deriveSubscriptionEntitlement(userId, userSubscriptions, null),
    });
    const actual = entitlementByUserId.get(userId);

    if (actual && (actual.source === "admin_grant" || actual.source === "admin_revoke" || actual.source === "owner")) {
      continue;
    }

    if (!actual) {
      items.push({
        itemKey: makeItemKey(["missing_entitlement", userId]),
        mismatchType: "missing_entitlement_projection",
        severity: expected.hasAccess ? "critical" : "warning",
        entityType: "user",
        whopUserId: userId,
        whopMembershipId: null,
        expectedAccess: expected.hasAccess,
        actualAccess: null,
        details: {
          expectedSource: expected.source,
          expectedExpiresAt: expected.expiresAt,
        },
      });
      continue;
    }

    const actualEffective = materializeEffectiveEntitlement({
      whopUserId: userId,
      ownerUserId,
      stored: actual,
    });

    if (
      actualEffective.hasAccess !== expected.hasAccess ||
      actualEffective.source !== expected.source ||
      (actualEffective.expiresAt ?? null) !== (expected.expiresAt ?? null)
    ) {
      items.push({
        itemKey: makeItemKey(["entitlement_projection", userId]),
        mismatchType: "entitlement_projection_mismatch",
        severity: expected.hasAccess !== actualEffective.hasAccess ? "critical" : "warning",
        entityType: "user",
        whopUserId: userId,
        whopMembershipId: null,
        expectedAccess: expected.hasAccess,
        actualAccess: actualEffective.hasAccess,
        details: {
          expectedSource: expected.source,
          actualSource: actualEffective.source,
          expectedExpiresAt: expected.expiresAt,
          actualExpiresAt: actualEffective.expiresAt,
        },
      });
    }
  }

  return items.sort((a, b) => a.itemKey.localeCompare(b.itemKey));
}
