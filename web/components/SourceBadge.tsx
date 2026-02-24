const SOURCE_COLORS: Record<string, string> = {
  action: "bg-orange-500/20 text-orange-300 border-orange-500/30",
  betql: "bg-cyan-500/20 text-cyan-300 border-cyan-500/30",
  covers: "bg-purple-500/20 text-purple-300 border-purple-500/30",
  dimers: "bg-pink-500/20 text-pink-300 border-pink-500/30",
  sportsline: "bg-green-500/20 text-green-300 border-green-500/30",
  oddstrader: "bg-yellow-500/20 text-yellow-300 border-yellow-500/30",
  vegasinsider: "bg-red-500/20 text-red-300 border-red-500/30",
};

const DEFAULT_COLOR = "bg-gray-500/20 text-gray-300 border-gray-500/30";

export default function SourceBadge({ source }: { source: string }) {
  const colorClass = SOURCE_COLORS[source] || DEFAULT_COLOR;
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${colorClass}`}
    >
      {source}
    </span>
  );
}
