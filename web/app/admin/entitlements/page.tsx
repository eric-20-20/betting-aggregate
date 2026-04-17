import { notFound } from "next/navigation";
import Link from "next/link";
import { isAdminRequest } from "@/lib/admin";
import { listEntitlements } from "@/lib/entitlements";
import { isSupabaseConfigured } from "@/lib/supabase";

export const dynamic = "force-dynamic";
export const metadata = { title: "Admin · Entitlements" };

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  return new Date(t).toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

export default async function AdminEntitlementsPage() {
  const isAdmin = await isAdminRequest();
  if (!isAdmin) notFound();

  const supabaseOn = isSupabaseConfigured();
  const rows = supabaseOn ? await listEntitlements(200) : [];

  const totals = {
    granted: rows.filter((r) => r.hasAccess).length,
    subscription: rows.filter((r) => r.source === "subscription").length,
    admin: rows.filter((r) => r.source === "admin_grant").length,
    owner: rows.filter((r) => r.source === "owner").length,
    none: rows.filter((r) => r.source === "none").length,
  };

  return (
    <div className="max-w-6xl mx-auto px-4 py-8 text-sm">
      <header className="flex items-baseline justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">User Entitlements</h1>
          <p className="text-gray-500 mt-1">
            Runtime access projection (<code>user_entitlements</code>). Derived from{" "}
            <code>subscriptions</code> on every webhook event.
          </p>
        </div>
        <nav className="flex gap-4 text-gray-400">
          <Link href="/admin/webhooks" className="hover:text-white">
            Webhooks →
          </Link>
        </nav>
      </header>

      {!supabaseOn && (
        <div className="rounded-lg border border-amber-600/40 bg-amber-900/20 text-amber-200 p-4 mb-6">
          Supabase is not configured in this environment. No entitlement data available.
        </div>
      )}

      <div className="grid grid-cols-5 gap-3 mb-6">
        {[
          { label: "Access granted", value: totals.granted, tone: "emerald" },
          { label: "Subscription", value: totals.subscription, tone: "emerald" },
          { label: "Admin grant", value: totals.admin, tone: "blue" },
          { label: "Owner", value: totals.owner, tone: "blue" },
          { label: "No access", value: totals.none, tone: "gray" },
        ].map((m) => (
          <div
            key={m.label}
            className={`rounded-lg border px-3 py-2 ${
              m.tone === "emerald"
                ? "border-emerald-500/30 bg-emerald-900/20"
                : m.tone === "blue"
                  ? "border-blue-500/30 bg-blue-900/20"
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
              <th className="px-3 py-2 text-left">whop_user_id</th>
              <th className="px-3 py-2 text-left">Access</th>
              <th className="px-3 py-2 text-left">Source</th>
              <th className="px-3 py-2 text-left">Expires</th>
              <th className="px-3 py-2 text-left">Last event</th>
              <th className="px-3 py-2 text-left">Updated</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-3 py-6 text-center text-gray-500">
                  No entitlement records yet.
                </td>
              </tr>
            ) : (
              rows.map((r) => (
                <tr key={r.whopUserId} className="border-t border-gray-800">
                  <td className="px-3 py-2 font-mono text-xs text-gray-300">
                    {r.whopUserId}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${
                        r.hasAccess
                          ? "bg-emerald-500/20 text-emerald-400"
                          : "bg-gray-700/40 text-gray-400"
                      }`}
                    >
                      {r.hasAccess ? "granted" : "denied"}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-gray-300">{r.source}</td>
                  <td className="px-3 py-2 text-gray-400 font-mono text-xs">
                    {formatDate(r.expiresAt)}
                  </td>
                  <td className="px-3 py-2 text-gray-500 font-mono text-xs">
                    {r.lastEventId || "—"}
                  </td>
                  <td className="px-3 py-2 text-gray-500 font-mono text-xs">
                    {formatDate(r.lastUpdatedAt)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <p className="mt-4 text-xs text-gray-500">
        Showing the most recently updated {rows.length} rows.
      </p>
    </div>
  );
}
