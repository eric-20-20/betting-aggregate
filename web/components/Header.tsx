import Link from "next/link";
import AuthButton from "./AuthButton";

const isAuthEnabled = Boolean(
  process.env.WHOP_CLIENT_ID && process.env.WHOP_CLIENT_SECRET
);

export default function Header() {
  return (
    <header className="border-b border-gray-800 bg-gray-950/80 backdrop-blur-sm sticky top-0 z-50">
      <div className="max-w-6xl mx-auto px-4 h-14 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2">
          <span className="text-emerald-400 font-bold text-lg">EdgePicks</span>
        </Link>

        <nav className="flex items-center gap-6">
          <Link
            href="/picks"
            className="text-sm text-gray-400 hover:text-white transition-colors"
          >
            Picks
          </Link>
          <Link
            href="/track-record"
            className="text-sm text-gray-400 hover:text-white transition-colors"
          >
            Track Record
          </Link>
          {isAuthEnabled && <AuthButton />}
        </nav>
      </div>
    </header>
  );
}
