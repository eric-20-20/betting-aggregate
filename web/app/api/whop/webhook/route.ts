import { NextRequest, NextResponse } from "next/server";
import crypto from "crypto";
import { clearAccessCache } from "@/lib/whop";
import { rateLimit } from "@/lib/rate-limit";
import {
  persistWebhookEvent,
  upsertSubscription,
  recomputeUserEntitlement,
  markWebhookProcessed,
  markWebhookFailed,
} from "@/lib/entitlements";
import { isSupabaseConfigured } from "@/lib/supabase";

const WEBHOOK_KEY = process.env.WHOP_WEBHOOK_KEY || "";
const limiter = rateLimit({ interval: 60_000, limit: 60 });

export async function GET() {
  return NextResponse.json({ ok: true, endpoint: "whop-webhook", method: "GET" });
}

function verifySignature(
  body: string,
  signature: string,
  timestamp: string
): boolean {
  if (!WEBHOOK_KEY) return false;

  const signedContent = `${timestamp}.${body}`;
  const expected = crypto
    .createHmac("sha256", WEBHOOK_KEY)
    .update(signedContent)
    .digest("base64");

  return signature === `v1,${expected}`;
}

/**
 * Membership event types we process end-to-end (persist + upsert +
 * recompute entitlement). Anything outside this set is still persisted
 * to `webhook_events` for audit but marked status='ignored'.
 */
const MEMBERSHIP_EVENT_TYPES = new Set([
  "membership.activated",
  "membership.canceled",
  "membership.renewed",
  "membership.updated",
  "membership.expired",
  "membership.went_valid",
  "membership.went_invalid",
]);

interface WhopEventBody {
  id?: string;
  type?: string;
  data?: {
    id?: string;
    user_id?: string;
    product_id?: string;
    status?: string;
    current_period_start?: string;
    current_period_end?: string;
    expires_at?: string;
    canceled_at?: string;
    user?: { id?: string };
    member?: { id?: string };
    membership?: { id?: string };
    [key: string]: unknown;
  };
  created_at?: string | number;
}

function extractIds(event: WhopEventBody): {
  userId: string | null;
  membershipId: string | null;
} {
  const d = event.data || {};
  const userId =
    (d.user?.id as string | undefined) ||
    (d.member?.id as string | undefined) ||
    (d.user_id as string | undefined) ||
    null;
  const membershipId =
    (d.membership?.id as string | undefined) ||
    (d.id as string | undefined) ||
    null;
  return { userId: userId || null, membershipId: membershipId || null };
}

export async function POST(request: NextRequest) {
  if (!WEBHOOK_KEY) {
    return NextResponse.json({ error: "Webhook not configured" }, { status: 500 });
  }

  // Rate limit on unauthenticated surface. This is per-instance; multi-
  // instance deployments should back this with Redis.
  const ip = request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || "unknown";
  const { success } = limiter.check(ip);
  if (!success) {
    return NextResponse.json(
      { error: "Too many requests" },
      { status: 429, headers: { "Retry-After": "60" } }
    );
  }

  const signature = request.headers.get("webhook-signature") || "";
  const timestamp = request.headers.get("webhook-timestamp") || "";
  const body = await request.text();

  const signatureVerified = verifySignature(body, signature, timestamp);
  if (!signatureVerified) {
    return NextResponse.json({ error: "Invalid signature" }, { status: 401 });
  }

  // Parse before persistence so we can record a useful row even for
  // malformed payloads.
  let event: WhopEventBody;
  try {
    event = JSON.parse(body) as WhopEventBody;
  } catch {
    return NextResponse.json({ error: "Invalid payload" }, { status: 400 });
  }

  const eventId = event.id;
  const eventType = event.type || "unknown";
  if (!eventId) {
    // Can't dedup without an id — reject. Whop always sends one in real events.
    return NextResponse.json({ error: "Missing event id" }, { status: 400 });
  }

  const { userId, membershipId } = extractIds(event);

  // If Supabase isn't configured, fall back to the legacy cache-clear
  // behavior so dev/preview environments still work. Log so it's visible.
  if (!isSupabaseConfigured()) {
    if (userId) clearAccessCache(userId);
    console.warn(
      "[webhook] Supabase not configured; event processed in legacy cache-clear mode",
      { eventId, eventType }
    );
    return NextResponse.json({ received: true, durable: false });
  }

  // 1) Persist raw event. Duplicate delivery is a no-op.
  let newlyInserted = false;
  try {
    newlyInserted = await persistWebhookEvent({
      eventId,
      eventType,
      signatureVerified,
      whopUserId: userId,
      whopMembershipId: membershipId,
      rawPayload: event,
    });
  } catch (err) {
    console.error("[webhook] persistWebhookEvent failed", err);
    // Return 500 so Whop retries. The DB is the source of truth — we
    // must not claim success if the raw record wasn't stored.
    return NextResponse.json({ error: "Internal error" }, { status: 500 });
  }

  if (!newlyInserted) {
    // Already recorded — idempotent re-delivery.
    return NextResponse.json({ received: true, duplicate: true });
  }

  // 2) For non-membership events, mark ignored and return.
  if (!MEMBERSHIP_EVENT_TYPES.has(eventType)) {
    try {
      // Update status via a direct supabase call — we already have the helper
      // for success; add a lightweight "ignored" marker.
      await markWebhookProcessed(eventId);
      // Override status to "ignored" for clarity in admin listings.
      const { getSupabase } = await import("@/lib/supabase");
      const client = getSupabase();
      if (client) {
        await client
          .from("webhook_events")
          .update({ status: "ignored" })
          .eq("id", eventId);
      }
    } catch (err) {
      console.error("[webhook] ignored-event status update failed", err);
    }
    return NextResponse.json({ received: true, processed: false });
  }

  // 3) Membership event — upsert subscription, recompute entitlement.
  if (!membershipId || !userId) {
    await markWebhookFailed(eventId, "missing userId or membershipId");
    return NextResponse.json({ error: "Missing membership identity" }, { status: 400 });
  }

  const d = event.data || {};
  const lastEventAt = typeof event.created_at === "string"
    ? event.created_at
    : new Date().toISOString();

  try {
    await upsertSubscription({
      whopMembershipId: membershipId,
      whopUserId: userId,
      whopProductId: (d.product_id as string) || null,
      status: (d.status as string) || eventType.split(".")[1] || "unknown",
      currentPeriodStart: (d.current_period_start as string) || null,
      currentPeriodEnd:
        (d.current_period_end as string) || (d.expires_at as string) || null,
      canceledAt: (d.canceled_at as string) || null,
      lastEventId: eventId,
      lastEventAt,
      rawMembership: d,
    });

    await recomputeUserEntitlement(userId, eventId);

    // Only clear the legacy in-memory cache AFTER Supabase writes commit.
    // This preserves the invariant that cache misses re-read a correct
    // state, not an in-flight one.
    clearAccessCache(userId);

    await markWebhookProcessed(eventId);
    return NextResponse.json({ received: true, processed: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("[webhook] processing failed", { eventId, eventType, msg });
    await markWebhookFailed(eventId, msg);
    // 500 triggers Whop retry. The raw event is already durably stored.
    return NextResponse.json({ error: "Internal error" }, { status: 500 });
  }
}
