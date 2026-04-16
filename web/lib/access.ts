import "server-only";

import { getServerSession } from "next-auth";
import { authOptions, isAuthEnabled } from "./auth";
import { hasAccess } from "./whop";
import { isAdminRequest } from "./admin";

/**
 * Resolve subscriber status for the current request.
 * Checks (in order): admin cookie, Whop OAuth session + active membership.
 * Returns `true` if any path grants access.
 */
export async function requireSubscriber(): Promise<boolean> {
  if (await isAdminRequest()) return true;

  if (!isAuthEnabled) return false;

  const session = await getServerSession(authOptions);
  const whopUserId = session?.whopUserId;
  if (!whopUserId) return false;

  return hasAccess(whopUserId);
}
