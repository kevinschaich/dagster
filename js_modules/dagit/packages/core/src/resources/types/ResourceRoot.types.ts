// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type ResourceRootQueryVariables = Types.Exact<{
  resourceSelector: Types.ResourceSelector;
}>;

export type ResourceRootQuery = {
  __typename: 'DagitQuery';
  topLevelResourceOrError:
    | {
        __typename: 'PythonError';
        message: string;
        stack: Array<string>;
        errorChain: Array<{
          __typename: 'ErrorChainLink';
          isExplicitLink: boolean;
          error: {__typename: 'PythonError'; message: string; stack: Array<string>};
        }>;
      }
    | {__typename: 'ResourceNotFoundError'}
    | {
        __typename: 'TopLevelResource';
        name: string;
        description: string | null;
        configFields: Array<{
          __typename: 'ConfigTypeField';
          name: string;
          description: string | null;
          configTypeKey: string;
          isRequired: boolean;
          defaultValueAsJson: string | null;
        }>;
        configuredValues: Array<{__typename: 'ConfiguredValue'; key: string; value: string}>;
      };
};
