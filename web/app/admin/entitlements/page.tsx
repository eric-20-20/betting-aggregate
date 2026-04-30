import Link from "next/link";
import { redirect } from "next/navigation";
import AdminNav from "@/components/admin/AdminNav";
import EntitlementOverrideForm from "@/components/admin/EntitlementOverrideForm";
import { isAdminRequest } from "@/lib/admin";
import { listEntitlements, listRecentAdminActions } from "@/lib/entitlements";
import { isSupabaseConfigured } from "@/lib/supabase";

export const dynamic = "force-dynamic";
export const metadata = { title: "Admin · Entitlements" };

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  return new Date(t).toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

function sourceTone(source: string): string {
  switch (source) {
    case "subscription":
      return "bg-emerald-500/20 text-emerald-300";
    case "admin_grant":
      return "bg-blue-500/20 text-blue-300";
    case "admin_revoke":
      return "bg-red-500/20 text-red-300";
    case "owner":
      return "bg-violet-500/20 text-violet-300";
    default:
      return "bg-gray-700/40 text-gray-300";
  }
}

interface PageProps {
  searchParams?: Promise<{
    source?: string;
    access?: string;
    q?: string;
  }>;
}

export default async function AdminEntitlementsPage({ searchParams }: PageProps) {
  if (!(await isAdminRequest())) redirect("/admin");

  const params = (await searchParams) || {};
  const supabaseOn = isSupabaseConfigured();
  const [rows, recentActions] = supabaseOn
    ? await Promise.all([listEntitlements(500), listRecentAdminActions(20)])
    : [[], []];

  const q = params.q?.trim().toLowerCase() || "";
  const filtered = rows.filter((row) => {
    if (params.source && row.source !== params.source) return false;
    if (params.access === "granted" && !row.hasAccess) return false;
    if (params.access === "denied" && row.hasAccess) return false;
    if (q && !row.whopUserId.toLowerCase().includes(q)) return false;
    return true;
  });

  const totals = {
    granted: rows.filter((row) => row.hasAccess).length,
    subscription: rows.filter((row) => row.source === "subscription").length,
    adminGrant: rows.filter((row) => row.source === "admin_grant").length,
    adminRevoke: rows.filter((row) => row.source === "admin_revoke").length,
    owner: rows.filter((row) => row.source === "owner").length,
  };

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 text-sm">
      <header className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">User Entitlements</h1>
          <p className="mt-1 max-w-3xl text-gray-500">
            Runtime access projection in <code>user_entitlements</code>. Effective precedence is
            owner &gt; admin override &gt; subscription.
          </p>
        </div>
        <AdminNav current="/admin/entitlements" />
      </header>

      {!supabaseOn ? (
        <div className="rounded-lg border border-amber-600/40 bg-amber-900/20 p-4 text-amber-200">
          Supabase is not configured in this environment. No entitlement data is available.
        </div>
      ) : (
        <>
          <div className="mb-6 grid gap-3 md:grid-cols-5">
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Granted</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.granted}</div>
            </div>
            <div className="rounded-lg border border-emerald-500/20 bg-emerald-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Subscription</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.subscription}</div>
            </div>
            <div className="rounded-lg border border-blue-500/20 bg-blue-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Admin grant</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.adminGrant}</div>
            </div>
            <div className="rounded-lg border border-red-500/20 bg-red-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Admin revoke</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.adminRevoke}</div>
            </div>
            <div className="rounded-lg border border-violet-500/20 bg-violet-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Owner</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.owner}</div>
            </div>
          </div>

          <form className="mb-6 grid gap-3 rounded-lg border border-gray-800 bg-gray-900/50 p-4 md:grid-cols-4">
            <input
              type="text"
              name="q"
              defaultValue={params.q || ""}
              placeholder="Filter by whop_user_id"
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white placeholder:text-gray-500"
            />
            <select
              name="source"
              defaultValue={params.source || ""}
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white"
            >
              <option value="">All sources</option>
              <option value="subscription">subscription</option>
              <option value="admin_grant">admin_grant</option>
              <option value="admin_revoke">admin_revoke</option>
              <option value="owner">owner</option>
              <option value="none">none</option>
            </select>
            <select
              name="access"
              defaultValue={params.access || ""}
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white"
            >
              <option value="">Any access state</option>
              <option value="granted">granted</option>
              <option value="denied">denied</option>
            </select>
            <div className="flex gap-2">
              <button
                type="submit"
                className="rounded bg-emerald-600 px-4 py-2 font-medium text-white hover:bg-emerald-500"
              >
                Apply filters
              </button>
              <Link
                href="/admin/entitlements"
                className="rounded border border-gray-700 px-4 py-2 text-gray-300 hover:text-white"
              >
                Reset
              </Link>
            </div>
          </form>

          <div className="mb-8 rounded-lg border border-gray-800 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-900 text-xs uppercase tracking-wide text-gray-400">
                <tr>
                  <th className="px-3 py-2 text-left">whop_user_id</th>
                  <th className="px-3 py-2 text-left">Access</th>
                  <th className="px-3 py-2 text-left">Source</th>
                  <th className="px-3 py-2 text-left">Expires</th>
                  <th className="px-3 py-2 text-left">Last event</th>
                  <th className="px-3 py-2 text-left">Updated</th>
                  <th className="px-3 py-2 text-left">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-3 py-6 text-center text-gray-500">
                      No entitlement records match the current filters.
                    </td>
                  </tr>
                ) : (
                  filtered.map((row) => (
                    <tr key={row.whopUserId} className="border-t border-gray-800 align-top">
                      <td className="px-3 py-2 font-mono text-xs text-gray-300">
                        <div>{row.whopUserId}</div>
                        <div className="mt-1">
                          <Link
                            href={`/admin/reconciliation?q=${encodeURIComponent(row.whopUserId)}`}
                            className="text-[11px] text-gray-500 hover:text-white"
                          >
                            View reconciliation
                          </Link>
                        </div>
                      </td>
                      <td className="px-3 py-2">
                        <span
                          className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${
                            row.hasAccess
                              ? "bg-emerald-500/20 text-emerald-400"
                              : "bg-gray-700/40 text-gray-300"
                          }`}
                        >
                          {row.hasAccess ? "granted" : "denied"}
                        </span>
                      </td>
                      <td className="px-3 py-2">
                        <span className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${sourceTone(row.source)}`}>
                          {row.source}
                        </span>
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-400">
                        {formatDate(row.expiresAt)}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-500">
                        {row.lastEventId || "—"}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-500">
                        {formatDate(row.lastUpdatedAt)}
                      </td>
                      <td className="px-3 py-2 min-w-[300px]">
                        {row.source === "owner" ? (
                          <p className="text-xs text-gray-500">Owner access cannot be overridden.</p>
                        ) : (
                          <EntitlementOverrideForm whopUserId={row.whopUserId} />
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          <section className="rounded-lg border border-gray-800 bg-gray-900/50 p-4">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-white">Recent admin actions</h2>
                <p className="text-xs text-gray-500">
                  Every manual entitlement mutation is recorded here with actor and reason.
                </p>
              </div>
              <Link href="/admin/reconciliation" className="text-sm text-gray-400 hover:text-white">
                Reconciliation →
              </Link>
            </div>
            <div className="overflow-hidden rounded-lg border border-gray-800">
              <table className="w-full">
                <thead className="bg-gray-950 text-xs uppercase tracking-wide text-gray-400">
                  <tr>
                    <th className="px-3 py-2 text-left">When</th>
                    <th className="px-3 py-2 text-left">User</th>
                    <th className="px-3 py-2 text-left">Action</th>
                    <th className="px-3 py-2 text-left">Reason</th>
                    <th className="px-3 py-2 text-left">Actor</th>
                  </tr>
                </thead>
                <tbody>
                  {recentActions.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="px-3 py-6 text-center text-gray-500">
                        No manual admin actions yet.
                      </td>
                    </tr>
                  ) : (
                    recentActions.map((action) => (
                      <tr key={action.id} className="border-t border-gray-800">
                        <td className="px-3 py-2 font-mono text-xs text-gray-500">
                          {formatDate(action.createdAt)}
                        </td>
                        <td className="px-3 py-2 font-mono text-xs text-gray-300">
                          {action.whopUserId}
                        </td>
                        <td className="px-3 py-2 text-gray-200">{action.action}</td>
                        <td className="px-3 py-2 text-gray-300">{action.reason}</td>
                        <td className="px-3 py-2 text-xs text-gray-500">{action.actorIdentifier}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
