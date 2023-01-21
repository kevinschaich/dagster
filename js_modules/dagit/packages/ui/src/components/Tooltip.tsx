// eslint-disable-next-line no-restricted-imports
import {Tooltip2, Tooltip2Props} from '@blueprintjs/popover2';
import deepmerge from 'deepmerge';
import React from 'react';
import styled, {createGlobalStyle, css} from 'styled-components/macro';

import {Colors} from './Colors';
import {FontFamily} from './styles';

export const GlobalTooltipStyle = createGlobalStyle`
  .dagit-tooltip .bp3-popover2-content {
      font-family: ${FontFamily.default};
      font-size: 12px;
      line-height: 16px;
      background: ${Colors.Gray900};
      color: ${Colors.Gray50};
      padding: 8px 16px;
  }

  .block-tooltip.bp3-popover2-target {
    display: block;
  }

  .dagit-tooltip-bare .bp3-popover2-content {
    padding: 0;
  }
`;

// Overwrite arrays instead of concatting them.
const overwriteMerge = (destination: any[], source: any[]) => source;

interface Props extends Tooltip2Props {
  display?: React.CSSProperties['display'];
  canShow?: boolean;
  useDisabledButtonTooltipFix?: boolean;
}

export const Tooltip: React.FC<Props> = (props) => {
  const {useDisabledButtonTooltipFix = false, children, display, canShow = true, ...rest} = props;

  const [isOpen, setIsOpen] = React.useState<undefined | boolean>(undefined);

  const divRef = React.useRef<HTMLDivElement>(null);

  React.useLayoutEffect(() => {
    let listener: null | ((e: MouseEvent) => void) = null;
    if (isOpen && useDisabledButtonTooltipFix) {
      listener = (e: MouseEvent) => {
        if (!divRef.current?.contains(e.target as HTMLDivElement)) {
          setIsOpen(false);
        }
      };
      document.body.addEventListener('mousemove', listener);
    }
    return () => {
      listener && document.body.removeEventListener('mousemove', listener);
    };
  }, [isOpen, useDisabledButtonTooltipFix]);

  if (!canShow) {
    return <>{children}</>;
  }

  const styledTooltip = (
    <StyledTooltip
      isOpen={isOpen}
      {...rest}
      minimal
      $display={display}
      popoverClassName={`dagit-tooltip ${props.popoverClassName}`}
      modifiers={deepmerge(
        {offset: {enabled: true, options: {offset: [0, 8]}}},
        props.modifiers || {},
        {arrayMerge: overwriteMerge},
      )}
    >
      {children}
    </StyledTooltip>
  );

  if (useDisabledButtonTooltipFix) {
    return (
      <div
        ref={divRef}
        onMouseEnter={() => {
          setIsOpen(true);
        }}
      >
        {styledTooltip}
      </div>
    );
  }
  return styledTooltip;
};

interface StyledTooltipProps {
  $display: React.CSSProperties['display'];
}

const StyledTooltip = styled(Tooltip2)<StyledTooltipProps>`
  ${({$display}) =>
    $display
      ? css`
          && {
            display: ${$display};
          }
        `
      : null}
`;
