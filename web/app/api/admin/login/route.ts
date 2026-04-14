import { NextRequest, NextResponse } from "next/server";
import { isValidAdminSecret, ADMIN_COOKIE_NAME } from "@/lib/admin";

export async function POST(request: NextRequest) {
  try {
    const { secret } = await request.json();
    if (!secret || typeof secret !== "string") {
      return NextResponse.json({ error: "Missing secret" }, { status: 400 });
    }

    if (!isValidAdminSecret(secret)) {
      return NextResponse.json({ error: "Invalid secret" }, { status: 401 });
    }

    const response = NextResponse.json({ ok: true });
    response.cookies.set(ADMIN_COOKIE_NAME, secret, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "strict",
      path: "/",
      maxAge: 60 * 60 * 24 * 7, // 7 days
    });

    return response;
  } catch {
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }
}
