import Link from "next/link";
import { redirect } from "next/navigation";
import AdminNav from "@/components/admin/AdminNav";
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

interface PageProps {
  searchParams?: Promise<{
    status?: string;
    sig?: string;
    q?: string;
  }>;
}

export default async function AdminWebhooksPage({ searchParams }: PageProps) {
  if (!(await isAdminRequest())) redirect("/admin");

  const params = (await searchParams) || {};
  const supabaseOn = isSupabaseConfigured();
  const events = supabaseOn ? await listRecentWebhookEvents(200) : [];
  const q = params.q?.trim().toLowerCase() || "";
  const filtered = events.filter((event) => {
    if (params.status && event.status !== params.status) return false;
    if (params.sig === "ok" && !event.signatureVerified) return false;
    if (params.sig === "bad" && event.signatureVerified) return false;
    if (
      q &&
      ![
        event.id,
        event.eventType,
        event.whopUserId || "",
        event.whopMembershipId || "",
      ]
        .join(" ")
        .toLowerCase()
        .includes(q)
    ) {
      return false;
    }
    return true;
  });

  const totals = {
    total: events.length,
    processed: events.filter((event) => event.status === "processed").length,
    failed: events.filter((event) => event.status === "failed").length,
    badSig: events.filter((event) => !event.signatureVerified).length,
  };

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 text-sm">
      <header className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Webhook Events</h1>
          <p className="mt-1 max-w-3xl text-gray-500">
            Recent Whop deliveries to <code>/api/whop/webhook</code>. Use this page to inspect
            processing status, signature verification, and links into affected entitlements.
          </p>
        </div>
        <AdminNav current="/admin/webhooks" />
      </header>

      {!supabaseOn ? (
        <div className="rounded-lg border border-amber-600/40 bg-amber-900/20 p-4 text-amber-200">
          Supabase is not configured. Durable webhook event storage is unavailable.
        </div>
      ) : (
        <>
          <div className="mb-6 grid gap-3 md:grid-cols-4">
            <div className="rounded-lg border border-gray-800 bg-gray-900/70 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Loaded</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.total}</div>
            </div>
            <div className="rounded-lg border border-emerald-500/20 bg-emerald-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Processed</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.processed}</div>
            </div>
            <div className="rounded-lg border border-red-500/20 bg-red-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Failed</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.failed}</div>
            </div>
            <div className="rounded-lg border border-amber-500/20 bg-amber-950/20 p-4">
              <div className="text-xs uppercase tracking-wide text-gray-500">Bad signature</div>
              <div className="mt-2 text-xl font-bold text-white">{totals.badSig}</div>
            </div>
          </div>

          <form className="mb-6 grid gap-3 rounded-lg border border-gray-800 bg-gray-900/50 p-4 md:grid-cols-4">
            <input
              type="text"
              name="q"
              defaultValue={params.q || ""}
              placeholder="Search event, user, membership"
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white placeholder:text-gray-500"
            />
            <select
              name="status"
              defaultValue={params.status || ""}
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white"
            >
              <option value="">All statuses</option>
              <option value="processed">processed</option>
              <option value="failed">failed</option>
              <option value="received">received</option>
              <option value="ignored">ignored</option>
            </select>
            <select
              name="sig"
              defaultValue={params.sig || ""}
              className="rounded border border-gray-700 bg-gray-950 px-3 py-2 text-white"
            >
              <option value="">Any signature state</option>
              <option value="ok">valid signature</option>
              <option value="bad">invalid signature</option>
            </select>
            <div className="flex gap-2">
              <button
                type="submit"
                className="rounded bg-emerald-600 px-4 py-2 font-medium text-white hover:bg-emerald-500"
              >
                Apply filters
              </button>
              <Link
                href="/admin/webhooks"
                className="rounded border border-gray-700 px-4 py-2 text-gray-300 hover:text-white"
              >
                Reset
              </Link>
            </div>
          </form>

          <div className="rounded-lg border border-gray-800 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-900 text-xs uppercase tracking-wide text-gray-400">
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
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={9} className="px-3 py-6 text-center text-gray-500">
                      No webhook events match the current filters.
                    </td>
                  </tr>
                ) : (
                  filtered.map((event) => (
                    <tr key={event.id} className="border-t border-gray-800 align-top">
                      <td className="px-3 py-2 font-mono text-xs text-gray-400 whitespace-nowrap">
                        {formatDate(event.receivedAt)}
                      </td>
                      <td className="px-3 py-2 text-gray-300">{event.eventType}</td>
                      <td className="px-3 py-2">
                        <span className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${statusTone(event.status)}`}>
                          {event.status}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-xs">
                        {event.signatureVerified ? (
                          <span className="text-emerald-400">ok</span>
                        ) : (
                          <span className="text-red-400">bad</span>
                        )}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-400">
                        {event.whopUserId ? (
                          <Link
                            href={`/admin/entitlements?q=${encodeURIComponent(event.whopUserId)}`}
                            className="hover:text-white"
                          >
                            {event.whopUserId}
                          </Link>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-400">
                        {event.whopMembershipId || "—"}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs text-gray-500">
                        {event.id}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap text-gray-500">
                        {formatDate(event.processedAt)}
                      </td>
                      <td
                        className="max-w-xs px-3 py-2 text-xs text-red-400"
                        title={event.processingError ?? ""}
                      >
                        {event.processingError || ""}
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
