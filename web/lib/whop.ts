import "server-only";

const WHOP_API_KEY = process.env.WHOP_API_KEY || "";
const WHOP_PRODUCT_ID = process.env.WHOP_PRODUCT_ID || "";
const WHOP_OWNER_USER_ID = process.env.WHOP_OWNER_USER_ID || "";

const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

interface CacheEntry {
  hasAccess: boolean;
  expiresAt: number;
}

const accessCache = new Map<string, CacheEntry>();

/**
 * Check whether a Whop user has access to the configured product.
 * Results are cached for 5 minutes to avoid excessive API calls.
 */
export async function hasAccess(whopUserId: string): Promise<boolean> {
  if (!WHOP_API_KEY || !WHOP_PRODUCT_ID) return false;

  // Owner bypass
  if (WHOP_OWNER_USER_ID && whopUserId === WHOP_OWNER_USER_ID) return true;

  // Check cache first
  const cached = accessCache.get(whopUserId);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.hasAccess;
  }

  try {
    const url = `https://api.whop.com/api/v1/memberships?user_ids=${whopUserId}&product_ids=${WHOP_PRODUCT_ID}&statuses=active`;
    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${WHOP_API_KEY}`,
      },
    });

    if (!res.ok) {
      console.error(`Whop access check failed: ${res.status}`);
      return false;
    }

    const data = await res.json();
    // If there are any valid memberships, user has access
    const result = Array.isArray(data?.data) && data.data.length > 0;

    accessCache.set(whopUserId, {
      hasAccess: result,
      expiresAt: Date.now() + CACHE_TTL_MS,
    });

    return result;
  } catch (err) {
    console.error("Whop access check error:", err);
    return false;
  }
}

/**
 * Clear cached access for a specific user (called from webhook handler).
 */
export function clearAccessCache(whopUserId: string): void {
  accessCache.delete(whopUserId);
}
