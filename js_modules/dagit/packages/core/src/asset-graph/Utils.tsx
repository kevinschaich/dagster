import {pathVerticalDiagonal} from '@vx/shape';

import {RunStatus} from '../graphql/types';

import {
  AssetNodeKeyFragment,
  AssetNodeLiveFragment,
  AssetNodeLiveMaterializationFragment,
  AssetNodeLiveFreshnessPolicyFragment,
  AssetNodeLiveFreshnessInfoFragment,
  AssetNodeLiveObservationFragment,
} from './types/AssetNode.types';
import {AssetNodeForGraphQueryFragment} from './types/useAssetGraphData.types';
import {
  AssetLatestInfoFragment,
  AssetLatestInfoRunFragment,
  AssetGraphLiveQuery,
} from './types/useLiveDataForAssetKeys.types';

type AssetNode = AssetNodeForGraphQueryFragment;
type AssetKey = AssetNodeKeyFragment;
type AssetLiveNode = AssetNodeLiveFragment;
type AssetLatestInfo = AssetLatestInfoFragment;

export const __ASSET_JOB_PREFIX = '__ASSET_JOB';

export function isHiddenAssetGroupJob(jobName: string) {
  return jobName.startsWith(__ASSET_JOB_PREFIX);
}

// IMPORTANT: We use this, rather than AssetNode.id throughout this file because
// the GraphQL interface exposes dependencyKeys, not dependencyIds. We also need
// ways to "build" GraphId's locally, they can't always be server-provided.
//
// This value is NOT the same as AssetNode.id values provided by the server,
// because JSON.stringify's whitespace behavior is different than Python's.
//
export type GraphId = string;
export const toGraphId = (key: {path: string[]}): GraphId => JSON.stringify(key.path);

export interface GraphNode {
  id: GraphId;
  assetKey: AssetKey;
  definition: AssetNode;
}

export interface GraphData {
  nodes: {[assetId: GraphId]: GraphNode};
  downstream: {[assetId: GraphId]: {[childAssetId: GraphId]: boolean}};
  upstream: {[assetId: GraphId]: {[parentAssetId: GraphId]: boolean}};
}

export const buildGraphData = (assetNodes: AssetNode[]) => {
  const data: GraphData = {
    nodes: {},
    downstream: {},
    upstream: {},
  };

  const addEdge = (upstreamGraphId: string, downstreamGraphId: string) => {
    if (upstreamGraphId === downstreamGraphId) {
      // Skip add edges for self-dependencies (eg: assets relying on older partitions of themselves)
      return;
    }
    data.downstream[upstreamGraphId] = {
      ...(data.downstream[upstreamGraphId] || {}),
      [downstreamGraphId]: true,
    };
    data.upstream[downstreamGraphId] = {
      ...(data.upstream[downstreamGraphId] || {}),
      [upstreamGraphId]: true,
    };
  };

  assetNodes.forEach((definition: AssetNode) => {
    const id = toGraphId(definition.assetKey);
    definition.dependencyKeys.forEach((key) => {
      addEdge(toGraphId(key), id);
    });
    definition.dependedByKeys.forEach((key) => {
      addEdge(id, toGraphId(key));
    });

    data.nodes[id] = {
      id,
      assetKey: definition.assetKey,
      definition,
    };
  });

  return data;
};

export const nodeDependsOnSelf = (node: GraphNode) => {
  const id = toGraphId(node.assetKey);
  return node.definition.dependedByKeys.some((d) => toGraphId(d) === id);
};

export const graphHasCycles = (graphData: GraphData) => {
  const nodes = new Set(Object.keys(graphData.nodes));
  const search = (stack: string[], node: string): boolean => {
    if (stack.indexOf(node) !== -1) {
      return true;
    }
    if (nodes.delete(node) === true) {
      const nextStack = stack.concat(node);
      return Object.keys(graphData.downstream[node] || {}).some((nextNode) =>
        search(nextStack, nextNode),
      );
    }
    return false;
  };
  let hasCycles = false;
  while (nodes.size !== 0 && !hasCycles) {
    hasCycles = search([], nodes.values().next().value);
  }
  return hasCycles;
};

export const buildSVGPath = pathVerticalDiagonal({
  source: (s: any) => s.source,
  target: (s: any) => s.target,
  x: (s: any) => s.x,
  y: (s: any) => s.y,
});

export interface LiveDataForNode {
  stepKey: string;
  unstartedRunIds: string[]; // run in progress and step not started
  inProgressRunIds: string[]; // run in progress and step in progress
  runWhichFailedToMaterialize: AssetLatestInfoRunFragment | null;
  lastMaterialization: AssetNodeLiveMaterializationFragment | null;
  lastMaterializationRunStatus: RunStatus | null; // only available if runWhichFailedToMaterialize is null
  freshnessPolicy: AssetNodeLiveFreshnessPolicyFragment | null;
  freshnessInfo: AssetNodeLiveFreshnessInfoFragment | null;
  lastObservation: AssetNodeLiveObservationFragment | null;
  currentLogicalVersion: string | null;
  projectedLogicalVersion: string | null;
  partitionStats: {numMaterialized: number; numPartitions: number} | null;
}

export const MISSING_LIVE_DATA: LiveDataForNode = {
  unstartedRunIds: [],
  inProgressRunIds: [],
  runWhichFailedToMaterialize: null,
  freshnessInfo: null,
  freshnessPolicy: null,
  lastMaterialization: null,
  lastMaterializationRunStatus: null,
  lastObservation: null,
  currentLogicalVersion: null,
  projectedLogicalVersion: null,
  partitionStats: null,
  stepKey: '',
};

export interface LiveData {
  [assetId: GraphId]: LiveDataForNode;
}

export interface AssetDefinitionsForLiveData {
  [id: string]: {
    definition: {
      partitionDefinition: string | null;
      jobNames: string[];
      opNames: string[];
    };
  };
}

export const buildLiveData = ({assetNodes, assetsLatestInfo}: AssetGraphLiveQuery) => {
  const data: LiveData = {};

  for (const liveNode of assetNodes) {
    const graphId = toGraphId(liveNode.assetKey);
    const assetLatestInfo = assetsLatestInfo.find(
      (r) => JSON.stringify(r.assetKey) === JSON.stringify(liveNode.assetKey),
    );

    data[graphId] = buildLiveDataForNode(liveNode, assetLatestInfo);
  }

  return data;
};

export const buildLiveDataForNode = (
  assetNode: AssetLiveNode,
  assetLatestInfo?: AssetLatestInfo,
): LiveDataForNode => {
  const lastMaterialization = assetNode.assetMaterializations[0] || null;
  const lastObservation = assetNode.assetObservations[0] || null;
  const currentLogicalVersion = assetNode.currentLogicalVersion;
  const projectedLogicalVersion = assetNode.projectedLogicalVersion;
  const latestRunForAsset = assetLatestInfo?.latestRun ? assetLatestInfo.latestRun : null;

  const runWhichFailedToMaterialize =
    (latestRunForAsset?.status === 'FAILURE' &&
      (!lastMaterialization || lastMaterialization.runId !== latestRunForAsset?.id) &&
      latestRunForAsset) ||
    null;

  return {
    lastMaterialization,
    lastMaterializationRunStatus:
      latestRunForAsset && lastMaterialization?.runId === latestRunForAsset?.id
        ? latestRunForAsset.status
        : null,
    lastObservation,
    currentLogicalVersion,
    projectedLogicalVersion,
    stepKey: assetNode.opNames[0],
    freshnessInfo: assetNode.freshnessInfo,
    freshnessPolicy: assetNode.freshnessPolicy,
    inProgressRunIds: assetLatestInfo?.inProgressRunIds || [],
    unstartedRunIds: assetLatestInfo?.unstartedRunIds || [],
    partitionStats: assetNode.partitionStats || null,
    runWhichFailedToMaterialize,
  };
};

export function tokenForAssetKey(key: {path: string[]}) {
  return key.path.join('/');
}

export function displayNameForAssetKey(key: {path: string[]}) {
  return key.path.join(' / ');
}
