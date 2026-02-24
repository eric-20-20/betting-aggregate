"use client";

import { useState } from "react";
import type { Play, LockedPick, Tier } from "@/lib/types";
import MarketTabs from "@/components/MarketTabs";
import PickCard from "@/components/PickCard";
import PickCardLocked from "@/components/PickCardLocked";
import PaywallPrompt from "@/components/PaywallPrompt";

type SubscriberProps = {
  isSubscriber: true;
  allPlays: Play[];
  playsByTier: Record<string, Play[]>;
  marketCounts: Record<string, number>;
  tierOrder: Tier[];
};

type FreeProps = {
  isSubscriber: false;
  teaserPicks: Play[];
  lockedPicks: LockedPick[];
  lockedByTier: Record<string, LockedPick[]>;
  marketCounts: Record<string, number>;
  tierOrder: Tier[];
};

type Props = SubscriberProps | FreeProps;

export default function PicksClientWrapper(props: Props) {
  const [activeTab, setActiveTab] = useState("all");

  if (props.isSubscriber) {
    return (
      <SubscriberView
        allPlays={props.allPlays}
        playsByTier={props.playsByTier}
        marketCounts={props.marketCounts}
        tierOrder={props.tierOrder}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
      />
    );
  }

  return (
    <FreeView
      teaserPicks={props.teaserPicks}
      lockedByTier={props.lockedByTier}
      marketCounts={props.marketCounts}
      tierOrder={props.tierOrder}
      activeTab={activeTab}
      setActiveTab={setActiveTab}
    />
  );
}

function SubscriberView({
  playsByTier,
  marketCounts,
  tierOrder,
  activeTab,
  setActiveTab,
}: {
  allPlays: Play[];
  playsByTier: Record<string, Play[]>;
  marketCounts: Record<string, number>;
  tierOrder: Tier[];
  activeTab: string;
  setActiveTab: (tab: string) => void;
}) {
  const filteredByTier: Record<string, Play[]> = {};
  for (const tier of tierOrder) {
    filteredByTier[tier] =
      activeTab === "all"
        ? playsByTier[tier] || []
        : (playsByTier[tier] || []).filter(
            (p) => p.signal.market_type === activeTab
          );
  }

  return (
    <>
      <div className="mb-6">
        <MarketTabs
          active={activeTab}
          onChange={setActiveTab}
          counts={marketCounts}
        />
      </div>

      {tierOrder.map((tier) => {
        const tierPlays = filteredByTier[tier] || [];
        if (tierPlays.length === 0) return null;
        return (
          <div key={tier} className="mb-6">
            <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wide mb-3">
              Tier {tier} ({tierPlays.length})
            </h3>
            <div className="space-y-3">
              {tierPlays.map((play) => (
                <PickCard key={play.signal.signal_id} play={play} />
              ))}
            </div>
          </div>
        );
      })}
    </>
  );
}

function FreeView({
  teaserPicks,
  lockedByTier,
  marketCounts,
  tierOrder,
  activeTab,
  setActiveTab,
}: {
  teaserPicks: Play[];
  lockedByTier: Record<string, LockedPick[]>;
  marketCounts: Record<string, number>;
  tierOrder: Tier[];
  activeTab: string;
  setActiveTab: (tab: string) => void;
}) {
  const filteredTeasers =
    activeTab === "all"
      ? teaserPicks
      : teaserPicks.filter((p) => p.signal.market_type === activeTab);

  const filteredLockedByTier: Record<string, LockedPick[]> = {};
  for (const tier of tierOrder) {
    filteredLockedByTier[tier] =
      activeTab === "all"
        ? lockedByTier[tier] || []
        : (lockedByTier[tier] || []).filter(
            (p) => p.market_type === activeTab
          );
  }

  const totalLocked = Object.values(filteredLockedByTier).reduce(
    (sum, arr) => sum + arr.length,
    0
  );

  return (
    <>
      <div className="mb-6">
        <MarketTabs
          active={activeTab}
          onChange={setActiveTab}
          counts={marketCounts}
        />
      </div>

      {/* Free teaser picks */}
      {filteredTeasers.length > 0 && (
        <div className="mb-4">
          <h2 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
            <span className="text-emerald-400">Free Preview</span>
          </h2>
          <div className="space-y-3">
            {filteredTeasers.map((play) => (
              <PickCard key={play.signal.signal_id} play={play} />
            ))}
          </div>
        </div>
      )}

      {/* Paywall */}
      {totalLocked > 0 && <PaywallPrompt lockedCount={totalLocked} />}

      {/* Locked picks by tier */}
      {tierOrder.map((tier) => {
        const tierPicks = filteredLockedByTier[tier] || [];
        if (tierPicks.length === 0) return null;
        return (
          <div key={tier} className="mb-6">
            <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wide mb-3">
              Tier {tier} ({tierPicks.length})
            </h3>
            <div className="space-y-2">
              {tierPicks.map((pick, i) => (
                <PickCardLocked key={`${tier}-${i}`} pick={pick} />
              ))}
            </div>
          </div>
        );
      })}
    </>
  );
}
