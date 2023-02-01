from typing import Optional

import pytest
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.decorators.source_asset_decorator import observable_source_asset
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.logical_version import (
    LogicalVersion,
    extract_logical_version_from_entry,
)
from dagster._core.definitions.unresolved_asset_job_definition import define_asset_job
from dagster._core.instance import DagsterInstance


@observable_source_asset
def foo(context):
    return LogicalVersion("foo")


@pytest.mark.parametrize(
    "asset_selection",
    [AssetSelection.keys("foo"), AssetSelection.all(), AssetSelection.source_assets(foo)],
    ids=["keys", "all", "source_assets"],
)
def test_define_source_asset_observation_job_with_asset_selection(asset_selection):
    assert Definitions(
        assets=[foo],
        jobs=[define_asset_job("source_asset_job", asset_selection)],
    ).get_job_def("source_asset_job")


def test_define_source_asset_job_with_source_assets():
    @observable_source_asset
    def foo(context):
        return LogicalVersion("foo")

    assert Definitions(
        assets=[foo],
        jobs=[define_asset_job("source_asset_job", [foo])],
    ).get_job_def("source_asset_job")


def _get_current_logical_version(
    key: AssetKey, instance: DagsterInstance
) -> Optional[LogicalVersion]:
    record = instance.get_latest_logical_version_record(AssetKey("foo"))
    assert record is not None
    return extract_logical_version_from_entry(record.event_log_entry)


def test_execute_source_asset_observation_job():
    executed = {}

    @observable_source_asset
    def foo(_context) -> LogicalVersion:
        executed["foo"] = True
        return LogicalVersion("alpha")

    instance = DagsterInstance.ephemeral()

    result = (
        Definitions(
            assets=[foo],
            jobs=[define_asset_job("source_asset_job", [foo])],
        )
        .get_job_def("source_asset_job")
        .execute_in_process(instance=instance)
    )

    assert result.success
    assert executed["foo"]
    assert _get_current_logical_version(AssetKey("foo"), instance) == LogicalVersion("alpha")
