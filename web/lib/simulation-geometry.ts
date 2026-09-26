type TopologyNode = {
  node_id: string;
  tier: string;
  latitude: number;
  longitude: number;
};

export type TierClusterSummary = {
  tier: string;
  nodeCount: number;
};

export function summarizeTierClusters(nodes: readonly TopologyNode[]): TierClusterSummary[] {
  const counts = new Map<string, number>();
  for (const node of nodes) {
    counts.set(node.tier, (counts.get(node.tier) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([tier, nodeCount]) => ({ tier, nodeCount }))
    .sort((left, right) => left.tier.localeCompare(right.tier));
}
