// Format helpers — safe to import from both server and client components

export function formatOdds(odds: number | null): string {
  if (odds === null) return "";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

export function formatWinPct(pct: number): string {
  return `${(pct * 100).toFixed(1)}%`;
}

export function formatRecord(wins: number, losses: number): string {
  return `${wins}-${losses}`;
}

export function formatPickSelection(signal: {
  selection: string;
  market_type: string;
  direction: string;
  atomic_stat: string | null;
  line: number | null;
  market_line?: number | null;
  away_team: string;
  home_team: string;
}): { main: string; detail: string } {
  const { selection, market_type, direction, atomic_stat, line, market_line } = signal;

  if (market_type === "player_prop") {
    const parts = selection.replace("NBA:", "").replace("NCAAB:", "").split("::");
    const playerRaw = parts[0] || "";
    const nameParts = playerRaw.split("_");
    const firstName = nameParts[0]?.charAt(0).toUpperCase() + ".";
    const lastName = nameParts
      .slice(1)
      .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
      .join(" ");
    const playerName = `${firstName} ${lastName}`;
    const stat = atomic_stat
      ? atomic_stat.charAt(0).toUpperCase() + atomic_stat.slice(1)
      : "";
    const dir = direction === "OVER" ? "Over" : "Under";
    const lineStr = line !== null ? ` ${line}` : "";
    return {
      main: playerName,
      detail: `${dir}${lineStr} ${stat}`,
    };
  }

  if (market_type === "spread") {
    // Use market_line (signed, from Odds API) when available so favorites show
    // as negative (e.g. LAC -6.5). Fall back to consensus line if unavailable.
    const displayLine = market_line != null ? market_line : line;
    const lineStr =
      displayLine !== null && displayLine !== undefined
        ? displayLine > 0
          ? ` +${displayLine}`
          : ` ${displayLine}`
        : "";
    return {
      main: `${selection}${lineStr}`,
      detail: "Spread",
    };
  }

  if (market_type === "total") {
    const dir = direction === "OVER" ? "Over" : "Under";
    const lineStr = line !== null ? ` ${line}` : "";
    return {
      main: `${dir}${lineStr}`,
      detail: "Total",
    };
  }

  if (market_type === "moneyline") {
    return {
      main: `${selection} ML`,
      detail: "Moneyline",
    };
  }

  return { main: selection, detail: market_type };
}
