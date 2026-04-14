"use client";

import { useSession, signIn } from "next-auth/react";

const WHOP_CHECKOUT_URL = process.env.NEXT_PUBLIC_WHOP_CHECKOUT_URL || "#";

export default function PaywallPrompt({
  lockedCount,
}: {
  lockedCount: number;
}) {
  const { data: session } = useSession();

  return (
    <div className="bg-gradient-to-br from-emerald-900/30 to-gray-800/60 border border-emerald-500/30 rounded-xl p-6 text-center my-6">
      <div className="text-3xl mb-3">🔒</div>
      <h3 className="text-xl font-bold text-white mb-2">
        {lockedCount} More Picks Available
      </h3>
      <p className="text-gray-400 mb-3 max-w-md mx-auto">
        Get full access to all daily consensus picks with detailed analysis.
      </p>
      <ul className="text-gray-500 text-sm mb-4 space-y-1 max-w-sm mx-auto text-left">
        <li>All A-tier picks with full factor breakdown</li>
        <li>Multi-source consensus scoring across 7+ experts</li>
        <li>Historical win rates and recent trend data</li>
        <li>Full graded pick history with P&L tracking</li>
      </ul>

      {session ? (
        // Logged in but not subscribed
        <a
          href={WHOP_CHECKOUT_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-2 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
        >
          Subscribe Now
          <ArrowIcon />
        </a>
      ) : (
        // Not logged in
        <div className="flex flex-col items-center gap-3">
          <button
            onClick={() => signIn("whop")}
            className="inline-flex items-center gap-2 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-6 py-3 rounded-lg transition-colors"
          >
            Sign In to Unlock
            <ArrowIcon />
          </button>
          <span className="text-gray-500 text-sm">
            or{" "}
            <a
              href={WHOP_CHECKOUT_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="text-emerald-400 hover:text-emerald-300 transition-colors"
            >
              subscribe on Whop
            </a>
          </span>
        </div>
      )}
    </div>
  );
}

function ArrowIcon() {
  return (
    <svg
      className="w-4 h-4"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M14 5l7 7m0 0l-7 7m7-7H3"
      />
    </svg>
  );
}
