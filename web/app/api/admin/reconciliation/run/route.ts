import { NextRequest, NextResponse } from "next/server";
import { isAdminRequest, getRequestIp } from "@/lib/admin";
import { rateLimit } from "@/lib/rate-limit";
import { runEntitlementReconciliation } from "@/lib/reconciliation";

const limiter = rateLimit({ interval: 60_000, limit: 5 });

export async function POST(request: NextRequest) {
  if (!(await isAdminRequest())) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const ip = getRequestIp(request);
  const { success } = limiter.check(`admin_reconciliation:${ip}`);
  if (!success) {
    return NextResponse.json({ error: "Too many requests" }, { status: 429 });
  }

  try {
    const run = await runEntitlementReconciliation({
      triggerSource: "manual",
      initiatedBy: "admin",
      ip,
    });
    return NextResponse.json({ ok: true, run });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to run reconciliation";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
