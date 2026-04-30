import { NextRequest, NextResponse } from "next/server";
import {
  applyAdminEntitlementAction,
  type AdminActionType,
} from "@/lib/entitlements";
import {
  getAdminActorIdentifier,
  getRequestIp,
  isAdminRequest,
} from "@/lib/admin";
import { rateLimit } from "@/lib/rate-limit";
import { clearAccessCache } from "@/lib/whop";

const limiter = rateLimit({ interval: 60_000, limit: 20 });

function isAction(value: unknown): value is AdminActionType {
  return value === "grant" || value === "revoke";
}

export async function POST(request: NextRequest) {
  if (!(await isAdminRequest())) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const ip = getRequestIp(request);
  const { success } = limiter.check(`admin_entitlement_override:${ip}`);
  if (!success) {
    return NextResponse.json({ error: "Too many requests" }, { status: 429 });
  }

  try {
    const body = (await request.json()) as {
      whopUserId?: unknown;
      action?: unknown;
      reason?: unknown;
    };

    if (typeof body.whopUserId !== "string" || !body.whopUserId.trim()) {
      return NextResponse.json({ error: "whopUserId is required" }, { status: 400 });
    }

    if (!isAction(body.action)) {
      return NextResponse.json({ error: "action must be grant or revoke" }, { status: 400 });
    }

    if (typeof body.reason !== "string" || body.reason.trim().length < 5) {
      return NextResponse.json(
        { error: "reason must be at least 5 characters" },
        { status: 400 }
      );
    }

    const entitlement = await applyAdminEntitlementAction({
      whopUserId: body.whopUserId.trim(),
      action: body.action,
      reason: body.reason.trim(),
      actorIdentifier: getAdminActorIdentifier(request),
      metadata: {
        ip,
        userAgent: request.headers.get("user-agent") || null,
      },
    });

    clearAccessCache(entitlement.whopUserId);
    return NextResponse.json({ ok: true, entitlement });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Override failed";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
