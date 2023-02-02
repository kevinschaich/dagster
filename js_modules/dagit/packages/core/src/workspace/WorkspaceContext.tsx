import {ApolloQueryResult, gql, useQuery} from '@apollo/client';
import sortBy from 'lodash/sortBy';
import * as React from 'react';

import {AppContext} from '../app/AppContext';
import {PYTHON_ERROR_FRAGMENT} from '../app/PythonErrorFragment';
import {PythonErrorFragment} from '../app/types/PythonErrorFragment.types';
import {PipelineSelector} from '../graphql/types';
import {useStateWithStorage} from '../hooks/useStateWithStorage';

import {REPOSITORY_INFO_FRAGMENT} from './RepositoryInformation';
import {buildRepoAddress} from './buildRepoAddress';
import {findRepoContainingPipeline} from './findRepoContainingPipeline';
import {RepoAddress} from './types';
import {
  RootWorkspaceQuery,
  WorkspaceLocationFragment,
  WorkspaceLocationNodeFragment,
  WorkspaceRepositoryFragment,
  WorkspaceScheduleFragment,
  WorkspaceSensorFragment,
} from './types/WorkspaceContext.types';

type Repository = WorkspaceRepositoryFragment;
type RepositoryLocation = WorkspaceLocationFragment;

export type WorkspaceRepositorySensor = WorkspaceSensorFragment;
export type WorkspaceRepositorySchedule = WorkspaceScheduleFragment;
export type WorkspaceRepositoryLocationNode = WorkspaceLocationNodeFragment;

export interface DagsterRepoOption {
  repositoryLocation: RepositoryLocation;
  repository: Repository;
}

export type WorkspaceState = {
  error: PythonErrorFragment | null;
  loading: boolean;
  locationEntries: WorkspaceRepositoryLocationNode[];
  allRepos: DagsterRepoOption[];
  visibleRepos: DagsterRepoOption[];

  refetch: () => Promise<ApolloQueryResult<RootWorkspaceQuery>>;
  toggleVisible: (repoAddresses: RepoAddress[]) => void;
};

export const WorkspaceContext = React.createContext<WorkspaceState>(
  new Error('WorkspaceContext should never be uninitialized') as any,
);

export const HIDDEN_REPO_KEYS = 'dagit.hidden-repo-keys';

const ROOT_WORKSPACE_QUERY = gql`
  query RootWorkspaceQuery {
    workspaceOrError {
      ... on Workspace {
        locationEntries {
          id
          ...WorkspaceLocationNode
        }
      }
      ...PythonErrorFragment
    }
  }

  fragment WorkspaceLocationNode on WorkspaceLocationEntry {
    id
    name
    loadStatus
    displayMetadata {
      ...WorkspaceDisplayMetadata
    }
    updatedTimestamp
    locationOrLoadError {
      ... on RepositoryLocation {
        id
        ...WorkspaceLocation
      }
      ...PythonErrorFragment
    }
  }

  fragment WorkspaceDisplayMetadata on RepositoryMetadata {
    key
    value
  }

  fragment WorkspaceLocation on RepositoryLocation {
    id
    isReloadSupported
    serverId
    name
    repositories {
      id
      ...WorkspaceRepository
    }
  }

  fragment WorkspaceRepository on Repository {
    id
    name
    pipelines {
      id
      name
      isJob
      isAssetJob
      pipelineSnapshotId
    }
    schedules {
      id
      ...WorkspaceSchedule
    }
    sensors {
      id
      ...WorkspaceSensor
    }
    partitionSets {
      id
      mode
      pipelineName
    }
    assetGroups {
      groupName
    }
    topLevelResources {
      name
    }
    ...RepositoryInfoFragment
  }

  fragment WorkspaceSchedule on Schedule {
    id
    cronSchedule
    executionTimezone
    mode
    name
    pipelineName
    scheduleState {
      id
      selectorId
      status
    }
  }

  fragment WorkspaceSensor on Sensor {
    id
    jobOriginId
    name
    targets {
      mode
      pipelineName
    }
    sensorState {
      id
      selectorId
      status
    }
  }

  ${PYTHON_ERROR_FRAGMENT}
  ${REPOSITORY_INFO_FRAGMENT}
`;

/**
 * A hook that supplies the current workspace state of Dagit, including the current
 * "active" repo based on the URL or localStorage, all fetched repositories available
 * in the workspace, and loading/error state for the relevant query.
 */
const useWorkspaceState = (): WorkspaceState => {
  const {data, loading, refetch} = useQuery<RootWorkspaceQuery>(ROOT_WORKSPACE_QUERY);
  const workspaceOrError = data?.workspaceOrError;

  const locationEntries = React.useMemo(
    () => (workspaceOrError?.__typename === 'Workspace' ? workspaceOrError.locationEntries : []),
    [workspaceOrError],
  );

  const {allRepos, error} = React.useMemo(() => {
    let allRepos: DagsterRepoOption[] = [];
    if (!workspaceOrError) {
      return {allRepos, error: null};
    }

    if (workspaceOrError.__typename === 'PythonError') {
      return {allRepos, error: workspaceOrError};
    }

    allRepos = sortBy(
      workspaceOrError.locationEntries.reduce((accum, locationEntry) => {
        if (locationEntry.locationOrLoadError?.__typename !== 'RepositoryLocation') {
          return accum;
        }
        const repositoryLocation = locationEntry.locationOrLoadError;
        const reposForLocation = repositoryLocation.repositories.map((repository) => {
          return {repository, repositoryLocation};
        });
        return [...accum, ...reposForLocation];
      }, [] as DagsterRepoOption[]),

      // Sort by repo location, then by repo
      (r) => `${r.repositoryLocation.name}:${r.repository.name}`,
    );

    return {error: null, allRepos};
  }, [workspaceOrError]);

  const [visibleRepos, toggleVisible] = useVisibleRepos(allRepos);

  return {
    refetch,
    loading: loading && !data, // Only "loading" on initial load.
    error,
    locationEntries,
    allRepos,
    visibleRepos,
    toggleVisible,
  };
};

/**
 * useVisibleRepos vends `[reposForKeys, toggleVisible]` and internally mirrors the current
 * selection into localStorage so that the default selection in new browser windows
 * is the repo currently active in your session.
 */
const validateHiddenKeys = (parsed: unknown) => (Array.isArray(parsed) ? parsed : []);

const useVisibleRepos = (
  allRepos: DagsterRepoOption[],
): [DagsterRepoOption[], WorkspaceState['toggleVisible']] => {
  const {basePath} = React.useContext(AppContext);

  const [oldHiddenKeys, setOldHiddenKeys] = useStateWithStorage<string[]>(
    HIDDEN_REPO_KEYS,
    validateHiddenKeys,
  );
  const [hiddenKeys, setHiddenKeys] = useStateWithStorage<string[]>(
    basePath + ':' + HIDDEN_REPO_KEYS,
    validateHiddenKeys,
  );

  // TODO: Remove this logic eventually...
  const migratedOldHiddenKeys = React.useRef(false);
  if (oldHiddenKeys.length && !migratedOldHiddenKeys.current) {
    setHiddenKeys(oldHiddenKeys);
    setOldHiddenKeys(undefined);
    migratedOldHiddenKeys.current = true;
  }

  const toggleVisible = React.useCallback(
    (repoAddresses: RepoAddress[]) => {
      repoAddresses.forEach((repoAddress) => {
        const key = `${repoAddress.name}:${repoAddress.location}`;

        setHiddenKeys((current) => {
          let nextHiddenKeys = [...(current || [])];
          if (nextHiddenKeys.includes(key)) {
            nextHiddenKeys = nextHiddenKeys.filter((k) => k !== key);
          } else {
            nextHiddenKeys = [...nextHiddenKeys, key];
          }
          return nextHiddenKeys;
        });
      });
    },
    [setHiddenKeys],
  );

  const visibleOptions = React.useMemo(() => {
    // If there's only one repo, skip the local storage check -- we have to show this one.
    if (allRepos.length === 1) {
      return allRepos;
    } else {
      return allRepos.filter((o) => !hiddenKeys.includes(getRepositoryOptionHash(o)));
    }
  }, [allRepos, hiddenKeys]);

  return [visibleOptions, toggleVisible];
};

// Public

export const getRepositoryOptionHash = (a: DagsterRepoOption) =>
  `${a.repository.name}:${a.repositoryLocation.name}`;

export const WorkspaceProvider: React.FC = (props) => {
  const {children} = props;
  const workspaceState = useWorkspaceState();

  return <WorkspaceContext.Provider value={workspaceState}>{children}</WorkspaceContext.Provider>;
};

export const useRepositoryOptions = () => {
  const {allRepos: options, loading, error} = React.useContext(WorkspaceContext);
  return {options, loading, error};
};

export const useRepository = (repoAddress: RepoAddress | null) => {
  const {options} = useRepositoryOptions();
  return findRepositoryAmongOptions(options, repoAddress) || null;
};

export const findRepositoryAmongOptions = (
  options: DagsterRepoOption[],
  repoAddress: RepoAddress | null,
) => {
  return repoAddress
    ? options.find(
        (option) =>
          option.repository.name === repoAddress.name &&
          option.repositoryLocation.name === repoAddress.location,
      )
    : null;
};

export const useActivePipelineForName = (pipelineName: string, snapshotId?: string) => {
  const {options} = useRepositoryOptions();
  const reposWithMatch = findRepoContainingPipeline(options, pipelineName, snapshotId);
  if (reposWithMatch.length) {
    const match = reposWithMatch[0];
    return match.repository.pipelines.find((pipeline) => pipeline.name === pipelineName) || null;
  }
  return null;
};

export const isThisThingAJob = (repo: DagsterRepoOption | null, pipelineOrJobName: string) => {
  const pipelineOrJob = repo?.repository.pipelines.find(
    (pipelineOrJob) => pipelineOrJob.name === pipelineOrJobName,
  );
  return !!pipelineOrJob?.isJob;
};

export const buildPipelineSelector = (
  repoAddress: RepoAddress | null,
  pipelineName: string,
  solidSelection?: string[],
) => {
  const repositorySelector = {
    repositoryName: repoAddress?.name || '',
    repositoryLocationName: repoAddress?.location || '',
  };

  return {
    ...repositorySelector,
    pipelineName,
    solidSelection,
  } as PipelineSelector;
};

export const optionToRepoAddress = (option: DagsterRepoOption) =>
  buildRepoAddress(option.repository.name, option.repository.location.name);
