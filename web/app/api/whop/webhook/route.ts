import { NextRequest, NextResponse } from "next/server";
import crypto from "crypto";
import { clearAccessCache } from "@/lib/whop";
import { rateLimit } from "@/lib/rate-limit";

const WEBHOOK_KEY = process.env.WHOP_WEBHOOK_KEY || "";
const limiter = rateLimit({ interval: 60_000, limit: 60 });

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

export async function POST(request: NextRequest) {
  if (!WEBHOOK_KEY) {
    return NextResponse.json({ error: "Webhook not configured" }, { status: 500 });
  }

  // Rate limiting
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

  if (!verifySignature(body, signature, timestamp)) {
    return NextResponse.json({ error: "Invalid signature" }, { status: 401 });
  }

  try {
    const event = JSON.parse(body);
    const type = event.type as string;
    const userId =
      event.data?.user?.id || event.data?.member?.id || null;

    if (
      userId &&
      (type === "membership.activated" ||
        type === "membership.canceled" ||
        type === "membership.renewed" ||
        type === "membership.updated")
    ) {
      clearAccessCache(userId);
    }

    return NextResponse.json({ received: true });
  } catch {
    return NextResponse.json({ error: "Invalid payload" }, { status: 400 });
  }
}
