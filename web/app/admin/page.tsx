import Link from "next/link";
import { isAdminRequest } from "@/lib/admin";
import AdminLoginForm from "./AdminLoginForm";

export const dynamic = "force-dynamic";
export const metadata = { title: "Admin" };

export default async function AdminIndexPage() {
  const isAdmin = await isAdminRequest();

  if (!isAdmin) {
    return (
      <div className="min-h-[60vh] flex items-center justify-center px-4">
        <AdminLoginForm />
      </div>
    );
  }

  return (
    <div className="max-w-3xl mx-auto px-4 py-10">
      <h1 className="text-2xl font-bold text-white mb-6">Admin</h1>
      <p className="text-gray-400 mb-8 text-sm">
        Signed in. Operational surfaces:
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Link
          href="/admin/entitlements"
          className="block rounded-lg border border-gray-800 bg-gray-900 hover:border-emerald-500/40 hover:bg-gray-900/80 transition-colors p-5"
        >
          <div className="text-lg font-semibold text-white">Entitlements</div>
          <p className="text-gray-400 text-sm mt-1">
            Runtime access table — who has access, from what source, and when
            it expires.
          </p>
        </Link>
        <Link
          href="/admin/webhooks"
          className="block rounded-lg border border-gray-800 bg-gray-900 hover:border-emerald-500/40 hover:bg-gray-900/80 transition-colors p-5"
        >
          <div className="text-lg font-semibold text-white">Webhooks</div>
          <p className="text-gray-400 text-sm mt-1">
            Last 100 Whop webhook events — status, signature check, any
            processing errors.
          </p>
        </Link>
      </div>
    </div>
  );
}
