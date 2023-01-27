import {Box} from '@dagster-io/ui';
import React from 'react';

import {RunStatus} from '../graphql/types';

import {AssetNode, AssetNodeMinimal} from './AssetNode';
import {LiveDataForNode} from './Utils';
import {getAssetNodeDimensions} from './layout';
import {AssetNodeFragment} from './types/AssetNode.types';

const ASSET_NODE_DEFINITION: AssetNodeFragment = {
  __typename: 'AssetNode',
  assetKey: {__typename: 'AssetKey', path: ['asset1']},
  computeKind: null,
  description: 'This is a test asset description',
  graphName: null,
  id: '["asset1"]',
  isObservable: false,
  isPartitioned: false,
  isSource: false,
  jobNames: ['job1'],
  opNames: ['asset1'],
  opVersion: '1',
};

const SOURCE_ASSET_NODE_DEFINITION: AssetNodeFragment = {
  ...ASSET_NODE_DEFINITION,
  assetKey: {__typename: 'AssetKey', path: ['source_asset']},
  description: 'This is a test source asset',
  id: '["source_asset"]',
  isObservable: true,
  isSource: true,
  jobNames: [],
  opNames: [],
};

const PARTITIONED_ASSET_NODE_DEFINITION: AssetNodeFragment = {
  ...ASSET_NODE_DEFINITION,
  assetKey: {__typename: 'AssetKey', path: ['asset1']},
  description: 'This is a partitioned asset description',
  id: '["asset1"]',
  isPartitioned: true,
};

// eslint-disable-next-line import/no-default-export
export default {component: AssetNode};

export const LiveStates = () => {
  const caseWithLiveData = (
    name: string,
    liveData: LiveDataForNode | undefined = undefined,
    def: AssetNodeFragment = ASSET_NODE_DEFINITION,
  ) => {
    const dimensions = getAssetNodeDimensions(def);
    return (
      <Box flex={{direction: 'column', gap: 0, alignItems: 'flex-start'}}>
        <div
          style={{position: 'relative', width: 280, height: dimensions.height, overflowY: 'hidden'}}
        >
          <AssetNode definition={def} selected={false} liveData={liveData} />
        </div>
        <div style={{position: 'relative', width: 280, height: 82}}>
          <div style={{position: 'absolute', width: 280, height: 82}}>
            <AssetNodeMinimal definition={def} selected={false} liveData={liveData} />
          </div>
        </div>
        <code>
          <strong>{name}</strong>
          <pre>{JSON.stringify(liveData, null, 2)}</pre>
        </code>
      </Box>
    );
  };
  return (
    <>
      <Box flex={{gap: 20, wrap: 'wrap', alignItems: 'flex-start'}}>
        {caseWithLiveData('No Live Data', undefined)}
        {caseWithLiveData('Run Started - Not Materializing Yet', {
          stepKey: 'asset1',
          unstartedRunIds: ['ABCDEF'],
          inProgressRunIds: [],
          lastMaterialization: null,
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: null,
          projectedLogicalVersion: null,
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}
        {caseWithLiveData('Run Started - Materializing', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: ['ABCDEF'],
          lastMaterialization: null,
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: null,
          projectedLogicalVersion: null,
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}

        {caseWithLiveData('Run Failed to Materialize', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: null,
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: {
            __typename: 'Run',
            id: 'ABCDEF',
            status: RunStatus.FAILURE,
            endTime: 1673301346,
          },
          currentLogicalVersion: null,
          projectedLogicalVersion: null,
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}

        {caseWithLiveData('Never Materialized', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: null,
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'INITIAL',
          projectedLogicalVersion: 'V_A',
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}

        {caseWithLiveData('Materialized', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'INITIAL',
          projectedLogicalVersion: 'V_A',
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}

        {caseWithLiveData('Materialized and Stale', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'V_A',
          projectedLogicalVersion: 'V_B',
          freshnessInfo: null,
          freshnessPolicy: null,
          partitionStats: null,
        })}
        {caseWithLiveData('Materialized and Stale and Late', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'V_A',
          projectedLogicalVersion: 'V_B',
          freshnessInfo: {
            __typename: 'AssetFreshnessInfo',
            currentMinutesLate: 12,
          },
          freshnessPolicy: {
            __typename: 'FreshnessPolicy',
            maximumLagMinutes: 10,
            cronSchedule: null,
          },
          partitionStats: null,
        })}
        {caseWithLiveData('Materialized and Stale and Fresh', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'V_A',
          projectedLogicalVersion: 'V_B',
          freshnessInfo: {
            __typename: 'AssetFreshnessInfo',
            currentMinutesLate: 0,
          },
          freshnessPolicy: {
            __typename: 'FreshnessPolicy',
            maximumLagMinutes: 10,
            cronSchedule: null,
          },
          partitionStats: null,
        })}
        {caseWithLiveData('Materialized and Fresh', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'V_B',
          projectedLogicalVersion: 'V_B',
          freshnessInfo: {
            __typename: 'AssetFreshnessInfo',
            currentMinutesLate: 0,
          },
          freshnessPolicy: {
            __typename: 'FreshnessPolicy',
            maximumLagMinutes: 10,
            cronSchedule: null,
          },
          partitionStats: null,
        })}
        {caseWithLiveData('Materialized and Late', {
          stepKey: 'asset1',
          unstartedRunIds: [],
          inProgressRunIds: [],
          lastMaterialization: {
            __typename: 'MaterializationEvent',
            runId: 'ABCDEF',
            timestamp: `${Date.now()}`,
          },
          lastMaterializationRunStatus: null,
          lastObservation: null,
          runWhichFailedToMaterialize: null,
          currentLogicalVersion: 'V_A',
          projectedLogicalVersion: 'V_A',
          freshnessInfo: {
            __typename: 'AssetFreshnessInfo',
            currentMinutesLate: 12,
          },
          freshnessPolicy: {
            __typename: 'FreshnessPolicy',
            maximumLagMinutes: 10,
            cronSchedule: null,
          },
          partitionStats: null,
        })}
      </Box>
      <Box flex={{gap: 20, wrap: 'wrap', alignItems: 'flex-start'}}>
        {caseWithLiveData('Source Asset - No Live Data', undefined, SOURCE_ASSET_NODE_DEFINITION)}
        {caseWithLiveData('Source Asset - Not Observable', undefined, {
          ...SOURCE_ASSET_NODE_DEFINITION,
          isObservable: false,
        })}
        {caseWithLiveData('Source Asset - Not Observable, No Description', undefined, {
          ...SOURCE_ASSET_NODE_DEFINITION,
          isObservable: false,
          description: null,
        })}
        {caseWithLiveData(
          'Source Asset - Never Observed',
          {
            stepKey: 'source_asset',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'INITIAL',
            projectedLogicalVersion: null,
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: null,
          },
          SOURCE_ASSET_NODE_DEFINITION,
        )}
        {caseWithLiveData(
          'Source Asset - Observation Running',
          {
            stepKey: 'source_asset',
            unstartedRunIds: [],
            inProgressRunIds: ['12345'],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'INITIAL',
            projectedLogicalVersion: null,
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: null,
          },
          SOURCE_ASSET_NODE_DEFINITION,
        )}
        {caseWithLiveData(
          'Source Asset - Observed, Stale',
          {
            stepKey: 'source_asset',
            unstartedRunIds: [],
            inProgressRunIds: ['12345'],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: {
              __typename: 'ObservationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'INITIAL',
            projectedLogicalVersion: 'DIFFERENT',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: null,
          },
          SOURCE_ASSET_NODE_DEFINITION,
        )}
        {caseWithLiveData(
          'Source Asset - Observed, Up To Date',
          {
            stepKey: 'source_asset',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: {
              __typename: 'ObservationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'DIFFERENT',
            projectedLogicalVersion: 'DIFFERENT',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: null,
          },
          SOURCE_ASSET_NODE_DEFINITION,
        )}
      </Box>
      <Box flex={{gap: 20, wrap: 'wrap', alignItems: 'flex-start'}}>
        {caseWithLiveData(
          'Partitioned Asset - Some Missing',
          {
            stepKey: 'partitioned_asset',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: {
              __typename: 'MaterializationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'DIFFERENT',
            projectedLogicalVersion: 'DIFFERENT',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: {
              numMaterialized: 5,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}
        {caseWithLiveData(
          'Partitioned Asset - None Missing',
          {
            stepKey: 'partitioned_asset',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: {
              __typename: 'MaterializationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'DIFFERENT',
            projectedLogicalVersion: 'DIFFERENT',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: {
              numMaterialized: 1500,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}

        {caseWithLiveData(
          'Never Materialized',
          {
            stepKey: 'asset1',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'INITIAL',
            projectedLogicalVersion: 'V_A',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: {
              numMaterialized: 0,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}

        {caseWithLiveData(
          'Partitioned Asset - Stale',
          {
            stepKey: 'asset1',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: {
              __typename: 'MaterializationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'V_A',
            projectedLogicalVersion: 'V_B',
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: {
              numMaterialized: 1500,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}

        {caseWithLiveData(
          'Partitioned Asset - Stale and Late',
          {
            stepKey: 'asset1',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: {
              __typename: 'MaterializationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'V_A',
            projectedLogicalVersion: 'V_B',
            freshnessInfo: {
              __typename: 'AssetFreshnessInfo',
              currentMinutesLate: 12,
            },
            freshnessPolicy: {
              __typename: 'FreshnessPolicy',
              maximumLagMinutes: 10,
              cronSchedule: null,
            },
            partitionStats: {
              numMaterialized: 1500,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}

        {caseWithLiveData(
          'Partitioned Asset - Stale and Fresh',
          {
            stepKey: 'asset1',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: {
              __typename: 'MaterializationEvent',
              runId: 'ABCDEF',
              timestamp: `${Date.now()}`,
            },
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: null,
            currentLogicalVersion: 'V_A',
            projectedLogicalVersion: 'V_B',
            freshnessInfo: {
              __typename: 'AssetFreshnessInfo',
              currentMinutesLate: 0,
            },
            freshnessPolicy: {
              __typename: 'FreshnessPolicy',
              maximumLagMinutes: 10,
              cronSchedule: null,
            },
            partitionStats: {
              numMaterialized: 1500,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}

        {caseWithLiveData(
          'Partitioned Asset - Last Run Failed',
          {
            stepKey: 'asset1',
            unstartedRunIds: [],
            inProgressRunIds: [],
            lastMaterialization: null,
            lastMaterializationRunStatus: null,
            lastObservation: null,
            runWhichFailedToMaterialize: {
              __typename: 'Run',
              id: 'ABCDEF',
              status: RunStatus.FAILURE,
              endTime: 1673301346,
            },
            currentLogicalVersion: null,
            projectedLogicalVersion: null,
            freshnessInfo: null,
            freshnessPolicy: null,
            partitionStats: {
              numMaterialized: 1500,
              numPartitions: 1500,
            },
          },
          PARTITIONED_ASSET_NODE_DEFINITION,
        )}
      </Box>
    </>
  );
  return;
};
