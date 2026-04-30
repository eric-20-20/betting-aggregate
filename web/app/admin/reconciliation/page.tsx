import Link from "next/link";
import { redirect } from "next/navigation";
import AdminNav from "@/components/admin/AdminNav";
import ReconciliationRunButton from "@/components/admin/ReconciliationRunButton";
import { isAdminRequest } from "@/lib/admin";
import {
  getLatestReconciliationRun,
  listUnresolvedReconciliationItems,
} from "@/lib/reconciliation";
import { isSupabaseConfigured } from "@/lib/supabase";

export const dynamic = "force-dynamic";
export const metadata = { title: "Admin · Reconciliation" };

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  return new Date(t).toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

function toneForSeverity(severity: string): string {
  switch (severity) {
    case "critical":
      return "bg-red-500/20 text-red-300";
    case "warning":
      return "bg-amber-500/20 text-amber-300";
    default:
      return "bg-blue-500/20 text-blue-300";
  }
}

interface PageProps {
  searchParams?: Promise<{ q?: string }>;
}

export default async function AdminReconciliationPage({ searchParams }: PageProps) {
  if (!(await isAdminRequest())) redirect("/admin");

  const params = (await searchParams) || {};
  const supabaseOn = isSupabaseConfigured();
  const [latestRun, unresolvedItems] = supabaseOn
    ? await Promise.all([
        getLatestReconciliationRun(),
        listUnresolvedReconciliationItems(100),
      ])
    : [null, []];
  const q = params.q?.trim().toLowerCase() || "";
  const filteredItems = unresolvedItems.filter((item) => {
    if (!q) return true;
    return [item.whopUserId || "", item.whopMembershipId || "", item.mismatchType]
      .join(" ")
      .toLowerCase()
      .includes(q);
  });

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 text-sm">
      <header className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Entitlement Reconciliation</h1>
          <p className="mt-1 max-w-3xl text-gray-500">
            Compare Whop memberships against local <code>subscriptions</code> and runtime
            <code> user_entitlements</code>. Each run records a durable summary and itemized drift.
          </p>
        </div>
        <div className="flex flex-col items-start gap-3 lg:items-end">
          <AdminNav current="/admin/reconciliation" />
          <ReconciliationRunButton />
        </div>
      </header>

      {!supabaseOn ? (
        <div className="rounded-lg border border-amber-600/40 bg-amber-900/20 p-4 text-amber-200">
          Supabase is not configured. Reconciliation is unavailable in this environment.
        </div>
      ) : (
        <>
          <div className="mb-6 grid gap-3 md:grid-cols-5">
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Latest run</div>
              <div className="mt-2 text-lg font-semibold text-white">
                {latestRun ? `#${latestRun.id}` : "None"}
              </div>
              <div className="mt-1 text-xs text-gray-500">
                {latestRun ? formatDate(latestRun.startedAt) : "No runs recorded"}
              </div>
            </div>
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Mismatch items</div>
              <div className="mt-2 text-lg font-semibold text-white">
                {latestRun?.mismatchCount ?? 0}
              </div>
            </div>
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Unresolved</div>
              <div className="mt-2 text-lg font-semibold text-white">
                {latestRun?.unresolvedCount ?? unresolvedItems.length}
              </div>
            </div>
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Whop memberships</div>
              <div className="mt-2 text-lg font-semibold text-white">
                {latestRun?.totalWhopMemberships ?? 0}
              </div>
            </div>
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Subscriptions / entitlements</div>
              <div className="mt-2 text-lg font-semibold text-white">
                {(latestRun?.totalSubscriptionRows ?? 0)}/{latestRun?.totalEntitlementRows ?? 0}
              </div>
            </div>
          </div>

          {latestRun ? (
            <div className="mb-6 rounded-lg border border-gray-800 bg-gray-900/60 p-4">
              <div className="flex flex-wrap items-center gap-3">
                <span
                  className={`rounded px-2 py-1 text-xs font-semibold ${
                    latestRun.status === "completed"
                      ? "bg-emerald-500/20 text-emerald-300"
                      : latestRun.status === "failed"
                        ? "bg-red-500/20 text-red-300"
                        : "bg-amber-500/20 text-amber-300"
                  }`}
                >
                  {latestRun.status}
                </span>
                <span className="text-gray-400">triggered by {latestRun.initiatedBy || "system"}</span>
                <span className="text-gray-500">finished {formatDate(latestRun.finishedAt)}</span>
              </div>
              {latestRun.errorMessage ? (
                <p className="mt-3 text-sm text-red-400">{latestRun.errorMessage}</p>
              ) : null}
              <div className="mt-4 flex flex-wrap gap-2 text-xs text-gray-400">
                <span>Critical: {Number(latestRun.summary.criticalCount || 0)}</span>
                <span>Warning: {Number(latestRun.summary.warningCount || 0)}</span>
                <span>Info: {Number(latestRun.summary.infoCount || 0)}</span>
              </div>
            </div>
          ) : null}

          <form className="mb-4 flex gap-2 rounded-lg border border-gray-800 bg-gray-900/50 p-4">
            <input
              type="text"
              name="q"
              defaultValue={params.q || ""}
              placeholder="Filter by user, membership, mismatch type"
              className="min-w-0 flex-1 rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white placeholder:text-gray-500"
            />
            <button
              type="submit"
              className="rounded bg-emerald-600 px-4 py-2 font-medium text-white hover:bg-emerald-500"
            >
              Filter
            </button>
            <Link
              href="/admin/reconciliation"
              className="rounded border border-gray-700 px-4 py-2 text-gray-300 hover:text-white"
            >
              Reset
            </Link>
          </form>

          <div className="rounded-lg border border-gray-800 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-900 text-xs uppercase tracking-wide text-gray-400">
                <tr>
                  <th className="px-3 py-2 text-left">Type</th>
                  <th className="px-3 py-2 text-left">Severity</th>
                  <th className="px-3 py-2 text-left">User</th>
                  <th className="px-3 py-2 text-left">Membership</th>
                  <th className="px-3 py-2 text-left">Expected</th>
                  <th className="px-3 py-2 text-left">Actual</th>
                  <th className="px-3 py-2 text-left">Details</th>
                </tr>
              </thead>
              <tbody>
                {filteredItems.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-3 py-6 text-center text-gray-500">
                      No unresolved mismatches.
                    </td>
                  </tr>
                ) : (
                  filteredItems.map((item) => (
                    <tr key={item.id} className="border-t border-gray-800 align-top">
                      <td className="px-3 py-2 text-gray-200">{item.mismatchType}</td>
                      <td className="px-3 py-2">
                        <span className={`rounded px-2 py-0.5 text-xs font-semibold ${toneForSeverity(item.severity)}`}>
                          {item.severity}
                        </span>
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-300">
                        {item.whopUserId ? (
                          <Link
                            href={`/admin/entitlements?q=${encodeURIComponent(item.whopUserId)}`}
                            className="hover:text-white"
                          >
                            {item.whopUserId}
                          </Link>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-500">
                        {item.whopMembershipId || "—"}
                      </td>
                      <td className="px-3 py-2 text-gray-300">
                        {item.expectedAccess == null ? "—" : item.expectedAccess ? "grant" : "deny"}
                      </td>
                      <td className="px-3 py-2 text-gray-300">
                        {item.actualAccess == null ? "—" : item.actualAccess ? "grant" : "deny"}
                      </td>
                      <td className="px-3 py-2 text-xs text-gray-400">
                        <pre className="whitespace-pre-wrap break-words">
                          {JSON.stringify(item.details, null, 2)}
                        </pre>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
