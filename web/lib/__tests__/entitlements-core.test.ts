import test from "node:test";
import assert from "node:assert/strict";
import {
  buildAdminOverrideEntitlement,
  detectReconciliationMismatches,
  deriveSubscriptionEntitlement,
  materializeEffectiveEntitlement,
  type EntitlementRecord,
  type SubscriptionSnapshot,
  type WhopMembershipSnapshot,
} from "../entitlements-core.ts";

test("override precedence is owner > admin override > subscription", () => {
  const subscriptionStored: EntitlementRecord = {
    whopUserId: "user_1",
    hasAccess: true,
    source: "subscription",
    expiresAt: "2099-01-01T00:00:00.000Z",
    lastEventId: "evt_1",
    lastUpdatedAt: "2026-04-20T00:00:00.000Z",
  };

  const owner = materializeEffectiveEntitlement({
    whopUserId: "owner_1",
    ownerUserId: "owner_1",
    stored: {
      ...subscriptionStored,
      whopUserId: "owner_1",
      source: "admin_revoke",
      hasAccess: false,
    },
  });
  assert.equal(owner.source, "owner");
  assert.equal(owner.hasAccess, true);

  const revoke = materializeEffectiveEntitlement({
    whopUserId: "user_1",
    ownerUserId: "owner_1",
    stored: {
      ...subscriptionStored,
      source: "admin_revoke",
      hasAccess: false,
      expiresAt: null,
    },
  });
  assert.equal(revoke.source, "admin_revoke");
  assert.equal(revoke.hasAccess, false);

  const grant = materializeEffectiveEntitlement({
    whopUserId: "user_1",
    ownerUserId: "owner_1",
    stored: {
      ...subscriptionStored,
      source: "admin_grant",
      hasAccess: true,
      expiresAt: null,
    },
  });
  assert.equal(grant.source, "admin_grant");
  assert.equal(grant.hasAccess, true);
});

test("admin actions build effective entitlement records with audit-friendly event ids", () => {
  const grant = buildAdminOverrideEntitlement({
    whopUserId: "user_2",
    action: "grant",
    nowIso: "2026-04-20T00:00:00.000Z",
    lastEventId: "admin_action:101",
  });
  assert.deepEqual(grant, {
    whopUserId: "user_2",
    hasAccess: true,
    source: "admin_grant",
    expiresAt: null,
    lastEventId: "admin_action:101",
    lastUpdatedAt: "2026-04-20T00:00:00.000Z",
  });

  const revoke = buildAdminOverrideEntitlement({
    whopUserId: "user_2",
    action: "revoke",
    nowIso: "2026-04-20T01:00:00.000Z",
    lastEventId: "admin_action:102",
  });
  assert.equal(revoke.source, "admin_revoke");
  assert.equal(revoke.hasAccess, false);
});

test("reconciliation detects membership drift and entitlement projection drift", () => {
  const whopMemberships: WhopMembershipSnapshot[] = [
    {
      id: "mem_missing_local",
      userId: "user_missing_local",
      productId: "prod_1",
      status: "active",
      currentPeriodStart: null,
      currentPeriodEnd: "2099-01-01T00:00:00.000Z",
      expiresAt: null,
      canceledAt: null,
    },
    {
      id: "mem_state_mismatch",
      userId: "user_mismatch",
      productId: "prod_1",
      status: "active",
      currentPeriodStart: null,
      currentPeriodEnd: "2099-01-01T00:00:00.000Z",
      expiresAt: null,
      canceledAt: null,
    },
  ];

  const subscriptions: SubscriptionSnapshot[] = [
    {
      whopMembershipId: "mem_state_mismatch",
      whopUserId: "user_mismatch",
      whopProductId: "prod_1",
      status: "expired",
      isActive: false,
      currentPeriodStart: null,
      currentPeriodEnd: null,
      canceledAt: null,
      lastEventId: "evt_1",
      lastEventAt: "2026-04-20T00:00:00.000Z",
    },
    {
      whopMembershipId: "mem_orphan_local",
      whopUserId: "user_orphan",
      whopProductId: "prod_1",
      status: "active",
      isActive: true,
      currentPeriodStart: null,
      currentPeriodEnd: "2099-01-01T00:00:00.000Z",
      canceledAt: null,
      lastEventId: "evt_2",
      lastEventAt: "2026-04-20T00:00:00.000Z",
    },
  ];

  const entitlementForMismatch = deriveSubscriptionEntitlement(
    "user_mismatch",
    subscriptions.filter((sub) => sub.whopUserId === "user_mismatch"),
    "evt_1"
  );

  const entitlements: EntitlementRecord[] = [
    {
      ...entitlementForMismatch,
      hasAccess: true,
      source: "subscription",
      expiresAt: "2099-01-01T00:00:00.000Z",
      lastUpdatedAt: "2026-04-20T00:00:00.000Z",
    },
  ];

  const items = detectReconciliationMismatches({
    whopMemberships,
    subscriptions,
    entitlements,
  });

  const types = items.map((item) => item.mismatchType);
  assert.ok(types.includes("missing_subscription_for_whop_membership"));
  assert.ok(types.includes("subscription_state_mismatch"));
  assert.ok(types.includes("subscription_missing_in_whop"));
  assert.ok(types.includes("missing_entitlement_projection"));
  assert.ok(types.includes("entitlement_projection_mismatch"));
});
