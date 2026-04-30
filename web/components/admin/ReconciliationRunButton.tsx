"use client";

import { useState } from "react";

export default function ReconciliationRunButton() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleRun() {
    setLoading(true);
    setError("");
    try {
      const res = await fetch("/api/admin/reconciliation/run", { method: "POST" });
      const body = (await res.json()) as { error?: string };
      if (!res.ok) {
        setError(body.error || "Failed to run reconciliation");
        setLoading(false);
        return;
      }
      window.location.reload();
    } catch {
      setError("Failed to run reconciliation");
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col items-end gap-2">
      <button
        type="button"
        onClick={handleRun}
        disabled={loading}
        className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-emerald-500 disabled:opacity-60"
      >
        {loading ? "Running..." : "Run reconciliation"}
      </button>
      {error ? <p className="text-xs text-red-400">{error}</p> : null}
    </div>
  );
}
