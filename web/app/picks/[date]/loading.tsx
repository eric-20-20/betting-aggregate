export default function PicksLoading() {
  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      {/* Header skeleton */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="h-7 w-36 bg-gray-800 rounded animate-pulse" />
          <div className="h-4 w-48 bg-gray-800/60 rounded animate-pulse mt-2" />
        </div>
        <div className="flex gap-3">
          <div className="h-7 w-20 bg-gray-800 rounded-lg animate-pulse" />
          <div className="h-7 w-24 bg-gray-800 rounded-lg animate-pulse" />
        </div>
      </div>

      {/* Tab bar skeleton */}
      <div className="h-10 w-full bg-gray-800/60 rounded-lg animate-pulse mb-6" />

      {/* Card skeletons */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <div
            key={i}
            className="bg-gray-800/40 border border-gray-700/30 rounded-lg p-3 space-y-3"
          >
            <div className="flex justify-between">
              <div className="h-5 w-16 bg-gray-700/40 rounded animate-pulse" />
              <div className="h-4 w-24 bg-gray-700/40 rounded animate-pulse" />
            </div>
            <div className="h-6 w-3/4 bg-gray-700/40 rounded animate-pulse" />
            <div className="h-4 w-1/2 bg-gray-700/40 rounded animate-pulse" />
            <div className="h-4 w-2/3 bg-gray-700/40 rounded animate-pulse" />
          </div>
        ))}
      </div>
    </div>
  );
}
