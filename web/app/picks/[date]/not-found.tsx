import Link from "next/link";

export default function PicksNotFound() {
  return (
    <div className="max-w-6xl mx-auto px-4 py-16 text-center">
      <h1 className="text-2xl font-bold text-white mb-4">Page Not Found</h1>
      <p className="text-gray-400 mb-6">
        This date doesn&apos;t exist or has an invalid format.
      </p>
      <Link
        href="/picks"
        className="bg-emerald-600 hover:bg-emerald-500 text-white px-6 py-2 rounded-lg transition-colors inline-block"
      >
        View Today&apos;s Picks
      </Link>
    </div>
  );
}
