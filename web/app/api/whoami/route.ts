import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import { hasAccess } from "@/lib/whop";

export async function GET() {
  const session = await getServerSession(authOptions);
  if (!session) {
    return NextResponse.json({ authenticated: false });
  }
  const whopUserId = (session as any).whopUserId as string | undefined;
  const access = whopUserId ? await hasAccess(whopUserId) : false;
  return NextResponse.json({
    authenticated: true,
    user: session.user,
    whopUserId: whopUserId || null,
    hasAccess: access,
  });
}
