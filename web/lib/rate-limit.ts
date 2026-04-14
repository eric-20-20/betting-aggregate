/**
 * In-memory sliding window rate limiter.
 * Sufficient for single-instance deployments (e.g., Vercel serverless).
 * For multi-instance, swap to Redis-backed implementation.
 */

interface RateLimitOptions {
  interval: number; // window in milliseconds
  limit: number;    // max requests per window
}

const store = new Map<string, number[]>();

export function rateLimit({ interval, limit }: RateLimitOptions) {
  return {
    check(identifier: string): { success: boolean; remaining: number } {
      const now = Date.now();
      const windowStart = now - interval;

      // Get or create timestamps array
      let timestamps = store.get(identifier) || [];

      // Prune expired entries
      timestamps = timestamps.filter((t) => t > windowStart);

      if (timestamps.length >= limit) {
        store.set(identifier, timestamps);
        return { success: false, remaining: 0 };
      }

      timestamps.push(now);
      store.set(identifier, timestamps);

      return { success: true, remaining: limit - timestamps.length };
    },
  };
}
