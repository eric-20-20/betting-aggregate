import Link from "next/link";

const LINKS = [
  { href: "/admin", label: "Overview" },
  { href: "/admin/entitlements", label: "Entitlements" },
  { href: "/admin/webhooks", label: "Webhooks" },
  { href: "/admin/reconciliation", label: "Reconciliation" },
];

export default function AdminNav({ current }: { current: string }) {
  return (
    <nav className="flex flex-wrap gap-2 text-sm">
      {LINKS.map((link) => (
        <Link
          key={link.href}
          href={link.href}
          className={`rounded-full border px-3 py-1.5 transition-colors ${
            current === link.href
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
              : "border-gray-800 bg-gray-900 text-gray-400 hover:text-white hover:border-gray-700"
          }`}
        >
          {link.label}
        </Link>
      ))}
    </nav>
  );
}
