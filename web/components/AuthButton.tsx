"use client";

import { useSession, signIn, signOut } from "next-auth/react";

export default function AuthButton() {
  const { data: session, status } = useSession();

  if (status === "loading") {
    return (
      <div className="w-20 h-8 bg-gray-800 rounded-lg animate-pulse" />
    );
  }

  if (session) {
    return (
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-400">
          {session.user?.name || "Member"}
        </span>
        <button
          onClick={() => signOut()}
          className="text-sm text-gray-500 hover:text-white transition-colors"
        >
          Sign Out
        </button>
      </div>
    );
  }

  return (
    <button
      onClick={() => signIn("whop")}
      className="bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium px-4 py-1.5 rounded-lg transition-colors"
    >
      Sign In
    </button>
  );
}
