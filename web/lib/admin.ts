import "server-only";

import crypto from "crypto";
import { cookies } from "next/headers";

const ADMIN_COOKIE_NAME = "admin_token";

/**
 * Check whether the current request has a valid admin cookie.
 */
export async function isAdminRequest(): Promise<boolean> {
  const adminSecret = process.env.ADMIN_SECRET;
  if (!adminSecret) return false;

  const cookieStore = await cookies();
  const token = cookieStore.get(ADMIN_COOKIE_NAME)?.value;
  if (!token) return false;

  // Constant-time comparison to prevent timing attacks
  const a = Buffer.from(token);
  const b = Buffer.from(adminSecret);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

/**
 * Check whether a raw token value matches the admin secret.
 */
export function isValidAdminSecret(token: string): boolean {
  const adminSecret = process.env.ADMIN_SECRET;
  if (!adminSecret) return false;

  const a = Buffer.from(token);
  const b = Buffer.from(adminSecret);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

export { ADMIN_COOKIE_NAME };
