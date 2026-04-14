import { NextRequest, NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions, isAuthEnabled } from "@/lib/auth";
import { hasAccess } from "@/lib/whop";
import { isAdminRequest } from "@/lib/admin";
import { getHistoryDay } from "@/lib/data";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ date: string }> }
) {
  const { date } = await params;

  // Auth check
  let isSubscriber = await isAdminRequest();

  if (!isSubscriber && isAuthEnabled) {
    const session = await getServerSession(authOptions);
    const whopUserId = session?.whopUserId;
    if (whopUserId) {
      isSubscriber = await hasAccess(whopUserId);
    }
  }

  if (!isSubscriber) {
    return NextResponse.json(
      { error: "Subscription required", message: "Sign in and subscribe to view pick history." },
      { status: 401 }
    );
  }

  // Validate date format
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return NextResponse.json({ error: "Invalid date" }, { status: 400 });
  }

  const data = await getHistoryDay(date);
  if (!data) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  return NextResponse.json(data);
}
