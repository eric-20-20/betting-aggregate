export default function HistoryLoading() {
  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      {/* Header skeleton */}
      <div className="mb-6">
        <div className="h-7 w-36 bg-gray-800 rounded animate-pulse" />
        <div className="h-4 w-56 bg-gray-800/60 rounded animate-pulse mt-2" />
      </div>

      {/* Summary stats skeleton */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-8">
        {Array.from({ length: 4 }).map((_, i) => (
          <div
            key={i}
            className="bg-gray-800/60 border border-gray-700/50 rounded-lg p-3 space-y-2"
          >
            <div className="h-3 w-16 bg-gray-700/40 rounded animate-pulse" />
            <div className="h-6 w-20 bg-gray-700/40 rounded animate-pulse" />
            <div className="h-4 w-12 bg-gray-700/40 rounded animate-pulse" />
          </div>
        ))}
      </div>

      {/* Tab skeleton */}
      <div className="h-9 w-64 bg-gray-800/60 rounded animate-pulse mb-5" />

      {/* Day rows skeleton */}
      <div className="space-y-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <div
            key={i}
            className="h-14 bg-gray-800/40 border border-gray-700/30 rounded-lg animate-pulse"
          />
        ))}
      </div>
    </div>
  );
}
