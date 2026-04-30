"use client";

import { useState } from "react";

export default function EntitlementOverrideForm({ whopUserId }: { whopUserId: string }) {
  const [action, setAction] = useState<"grant" | "revoke">("grant");
  const [reason, setReason] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError("");

    try {
      const res = await fetch("/api/admin/entitlements/override", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ whopUserId, action, reason }),
      });
      const body = (await res.json()) as { error?: string };
      if (!res.ok) {
        setError(body.error || "Override failed");
        setLoading(false);
        return;
      }
      window.location.reload();
    } catch {
      setError("Override failed");
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-2 rounded-lg border border-gray-800 bg-gray-950/60 p-3">
      <div className="flex gap-2">
        <select
          value={action}
          onChange={(event) => setAction(event.target.value as "grant" | "revoke")}
          className="rounded border border-gray-700 bg-gray-900 px-2 py-1 text-xs text-white"
        >
          <option value="grant">Grant</option>
          <option value="revoke">Revoke</option>
        </select>
        <input
          value={reason}
          onChange={(event) => setReason(event.target.value)}
          placeholder="Reason required"
          className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-900 px-2 py-1 text-xs text-white placeholder:text-gray-500"
        />
      </div>
      <div className="flex items-center justify-between gap-2">
        <p className="text-[11px] text-gray-500">
          Override precedence: owner &gt; admin override &gt; subscription.
        </p>
        <button
          type="submit"
          disabled={loading || reason.trim().length < 5}
          className="rounded bg-blue-600 px-2.5 py-1 text-xs font-medium text-white transition-colors hover:bg-blue-500 disabled:opacity-60"
        >
          {loading ? "Saving..." : "Apply"}
        </button>
      </div>
      {error ? <p className="text-[11px] text-red-400">{error}</p> : null}
    </form>
  );
}
