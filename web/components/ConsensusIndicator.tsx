export default function ConsensusIndicator({ count }: { count: number }) {
  const dots = Math.min(count, 3);
  const label = count >= 3 ? "3+ data points" : `${count} data point${count !== 1 ? "s" : ""}`;

  const dotColor =
    count >= 3
      ? "bg-emerald-400"
      : count === 2
        ? "bg-blue-400"
        : "bg-gray-500";

  const textColor =
    count >= 3
      ? "text-emerald-400"
      : count === 2
        ? "text-blue-400"
        : "text-gray-500";

  return (
    <div className="flex items-center gap-1.5">
      <div className="flex gap-0.5">
        {Array.from({ length: dots }).map((_, i) => (
          <span key={i} className={`w-1.5 h-1.5 rounded-full ${dotColor}`} />
        ))}
      </div>
      <span className={`text-xs ${textColor}`}>{label}</span>
    </div>
  );
}
