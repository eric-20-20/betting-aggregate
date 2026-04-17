import { notFound } from "next/navigation";
import Link from "next/link";
import { isAdminRequest } from "@/lib/admin";
import { listRecentWebhookEvents } from "@/lib/entitlements";
import { isSupabaseConfigured } from "@/lib/supabase";

export const dynamic = "force-dynamic";
export const metadata = { title: "Admin · Webhooks" };

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  return new Date(t).toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

function statusTone(status: string): string {
  switch (status) {
    case "processed":
      return "bg-emerald-500/20 text-emerald-400";
    case "failed":
      return "bg-red-500/20 text-red-400";
    case "received":
      return "bg-amber-500/20 text-amber-400";
    case "ignored":
      return "bg-gray-700/40 text-gray-400";
    default:
      return "bg-gray-700/40 text-gray-400";
  }
}

export default async function AdminWebhooksPage() {
  const isAdmin = await isAdminRequest();
  if (!isAdmin) notFound();

  const supabaseOn = isSupabaseConfigured();
  const events = supabaseOn ? await listRecentWebhookEvents(100) : [];

  const totals = {
    total: events.length,
    processed: events.filter((e) => e.status === "processed").length,
    failed: events.filter((e) => e.status === "failed").length,
    received: events.filter((e) => e.status === "received").length,
    ignored: events.filter((e) => e.status === "ignored").length,
  };

  return (
    <div className="max-w-6xl mx-auto px-4 py-8 text-sm">
      <header className="flex items-baseline justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Webhook Events</h1>
          <p className="text-gray-500 mt-1">
            Last 100 events received on <code>/api/whop/webhook</code>. Raw
            payloads are preserved in the <code>webhook_events</code> table for
            replay/debug.
          </p>
        </div>
        <nav className="flex gap-4 text-gray-400">
          <Link href="/admin/entitlements" className="hover:text-white">
            ← Entitlements
          </Link>
        </nav>
      </header>

      {!supabaseOn && (
        <div className="rounded-lg border border-amber-600/40 bg-amber-900/20 text-amber-200 p-4 mb-6">
          Supabase is not configured. Webhook events are being processed in
          legacy cache-clear mode without durable storage.
        </div>
      )}

      <div className="grid grid-cols-5 gap-3 mb-6">
        {[
          { label: "Total (100 max)", value: totals.total, tone: "gray" },
          { label: "Processed", value: totals.processed, tone: "emerald" },
          { label: "Failed", value: totals.failed, tone: "red" },
          { label: "Received", value: totals.received, tone: "amber" },
          { label: "Ignored", value: totals.ignored, tone: "gray" },
        ].map((m) => (
          <div
            key={m.label}
            className={`rounded-lg border px-3 py-2 ${
              m.tone === "emerald"
                ? "border-emerald-500/30 bg-emerald-900/20"
                : m.tone === "red"
                  ? "border-red-500/30 bg-red-900/20"
                  : m.tone === "amber"
                    ? "border-amber-500/30 bg-amber-900/20"
                    : "border-gray-700 bg-gray-800/40"
            }`}
          >
            <div className="text-xs uppercase tracking-wide text-gray-400">{m.label}</div>
            <div className="text-xl font-bold text-white">{m.value}</div>
          </div>
        ))}
      </div>

      <div className="rounded-lg border border-gray-800 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-900 text-gray-400 text-xs uppercase tracking-wide">
            <tr>
              <th className="px-3 py-2 text-left">Received</th>
              <th className="px-3 py-2 text-left">Type</th>
              <th className="px-3 py-2 text-left">Status</th>
              <th className="px-3 py-2 text-left">Sig</th>
              <th className="px-3 py-2 text-left">user_id</th>
              <th className="px-3 py-2 text-left">membership_id</th>
              <th className="px-3 py-2 text-left">Event id</th>
              <th className="px-3 py-2 text-left">Processed</th>
              <th className="px-3 py-2 text-left">Error</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 ? (
              <tr>
                <td colSpan={9} className="px-3 py-6 text-center text-gray-500">
                  No webhook events recorded yet.
                </td>
              </tr>
            ) : (
              events.map((e) => (
                <tr key={e.id} className="border-t border-gray-800 align-top">
                  <td className="px-3 py-2 text-gray-400 font-mono text-xs whitespace-nowrap">
                    {formatDate(e.receivedAt)}
                  </td>
                  <td className="px-3 py-2 text-gray-300">{e.eventType}</td>
                  <td className="px-3 py-2">
                    <span className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${statusTone(e.status)}`}>
                      {e.status}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-xs">
                    {e.signatureVerified ? (
                      <span className="text-emerald-400">ok</span>
                    ) : (
                      <span className="text-red-400">bad</span>
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-gray-400">
                    {e.whopUserId || "—"}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-gray-400">
                    {e.whopMembershipId || "—"}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-gray-500">
                    {e.id}
                  </td>
                  <td className="px-3 py-2 text-gray-500 font-mono text-xs whitespace-nowrap">
                    {formatDate(e.processedAt)}
                  </td>
                  <td className="px-3 py-2 text-red-400 text-xs max-w-xs truncate" title={e.processingError ?? ""}>
                    {e.processingError || ""}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
