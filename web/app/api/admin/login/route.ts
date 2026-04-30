import { NextRequest, NextResponse } from "next/server";
import {
  isValidAdminSecret,
  ADMIN_COOKIE_NAME,
  getAdminSessionToken,
  getRequestIp,
} from "@/lib/admin";
import { rateLimit } from "@/lib/rate-limit";

const limiter = rateLimit({ interval: 60_000, limit: 10 });

export async function POST(request: NextRequest) {
  try {
    const ip = getRequestIp(request);
    const { success } = limiter.check(`admin_login:${ip}`);
    if (!success) {
      return NextResponse.json({ error: "Too many requests" }, { status: 429 });
    }

    const { secret } = await request.json();
    if (!secret || typeof secret !== "string") {
      return NextResponse.json({ error: "Missing secret" }, { status: 400 });
    }

    if (!isValidAdminSecret(secret)) {
      return NextResponse.json({ error: "Invalid secret" }, { status: 401 });
    }

    const sessionToken = getAdminSessionToken();
    if (!sessionToken) {
      return NextResponse.json({ error: "Admin auth not configured" }, { status: 500 });
    }

    const response = NextResponse.json({ ok: true });
    response.cookies.set(ADMIN_COOKIE_NAME, sessionToken, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "strict",
      path: "/",
      maxAge: 60 * 60 * 24, // 24h
    });

    return response;
  } catch {
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }
}
