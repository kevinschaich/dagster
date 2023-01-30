import os
from pathlib import Path
from typing import Any, Optional, Sequence

import dagster._check as check
from dagster import AssetKey, AssetMaterialization
from dagster._config import Field
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.io_manager import IOManager, io_manager
from dagster._utils import mkdir_p

from dagstermill.factory import _clean_path_for_windows


class OutputNotebookIOManager(IOManager):
    def __init__(self, asset_key_prefix: Optional[Sequence[str]] = None):
        self.asset_key_prefix = asset_key_prefix if asset_key_prefix else []

    def handle_output(self, context: OutputContext, obj: bytes):
        raise NotImplementedError

    def load_input(self, context: InputContext) -> Any:
        raise NotImplementedError


class LocalOutputNotebookIOManager(OutputNotebookIOManager):
    """Built-in IO Manager for handling output notebook."""

    def __init__(self, base_dir: str, asset_key_prefix: Optional[Sequence[str]] = None):
        super(LocalOutputNotebookIOManager, self).__init__(asset_key_prefix=asset_key_prefix)
        self.base_dir = base_dir
        self.write_mode = "wb"
        self.read_mode = "rb"

    def _get_path(self, context: OutputContext) -> str:
        """Automatically construct filepath."""
        if context.has_asset_key:
            keys = context.get_asset_identifier()
        else:
            keys = context.get_run_scoped_output_identifier()
        return str(Path(self.base_dir, *keys).with_suffix(".ipynb"))

    def handle_output(self, context: OutputContext, obj: bytes):
        """obj: bytes."""
        check.inst_param(context, "context", OutputContext)

        # the output notebook itself is stored at output_file_path
        output_notebook_path = self._get_path(context)
        mkdir_p(os.path.dirname(output_notebook_path))
        with open(output_notebook_path, self.write_mode) as dest_file_obj:
            dest_file_obj.write(obj)

        metadata = {
            "Executed notebook": MetadataValue.notebook(
                _clean_path_for_windows(output_notebook_path)
            )
        }

        if context.has_asset_key:
            context.add_output_metadata(metadata)
        else:
            context.log_event(
                AssetMaterialization(
                    asset_key=AssetKey(
                        [*self.asset_key_prefix, f"{context.step_key}_output_notebook"]
                    ),
                    metadata=metadata,
                )
            )

    def load_input(self, context) -> bytes:
        check.inst_param(context, "context", InputContext)
        # pass output notebook to downstream ops as File Object
        output_context = check.not_none(context.upstream_output)  # type: ignore  # fmt: skip
        with open(self._get_path(output_context), self.read_mode) as file_obj:
            return file_obj.read()


@io_manager(
    config_schema={
        "asset_key_prefix": Field(str, is_required=False),
        "base_dir": Field(str, is_required=False),
    },
)
def local_output_notebook_io_manager(init_context):
    """Built-in IO Manager that handles output notebooks."""
    return LocalOutputNotebookIOManager(
        base_dir=init_context.resource_config.get(
            "base_dir", init_context.instance.storage_directory()
        ),
        asset_key_prefix=init_context.resource_config.get("asset_key_prefix", []),
    )
