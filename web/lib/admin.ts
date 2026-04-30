import "server-only";

import crypto from "crypto";
import { cookies } from "next/headers";
import type { NextRequest } from "next/server";

export const ADMIN_COOKIE_NAME = "admin_session";

function createAdminSessionToken(adminSecret: string): string {
  return crypto
    .createHash("sha256")
    .update(`admin-session:${adminSecret}`)
    .digest("hex");
}

export function getAdminSessionToken(): string | null {
  const adminSecret = process.env.ADMIN_SECRET;
  if (!adminSecret) return null;
  return createAdminSessionToken(adminSecret);
}

export async function isAdminRequest(): Promise<boolean> {
  const expected = getAdminSessionToken();
  if (!expected) return false;

  const cookieStore = await cookies();
  const token = cookieStore.get(ADMIN_COOKIE_NAME)?.value;
  if (!token) return false;

  const a = Buffer.from(token);
  const b = Buffer.from(expected);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

export function isValidAdminSecret(token: string): boolean {
  const adminSecret = process.env.ADMIN_SECRET;
  if (!adminSecret) return false;

  const a = Buffer.from(token);
  const b = Buffer.from(adminSecret);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

export function getRequestIp(request: NextRequest): string {
  return (
    request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    request.headers.get("x-real-ip") ||
    "unknown"
  );
}

export function getAdminActorIdentifier(request: NextRequest): string {
  const ip = getRequestIp(request);
  const userAgent = request.headers.get("user-agent") || "unknown_ua";
  return `admin_secret:${ip}:${userAgent.slice(0, 120)}`;
}
