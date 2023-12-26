import pytest
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

from scheduler.watcher.expandable_restapi_watcher import ExpandableRestAPIWatcher
from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor
from scheduler.upstream_sensor.xcom_query import XComQuery


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_expand(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
        expand_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )
    upstream_df = pd.DataFrame({
        "batch_id": ["doesnt_matter"] * 2 + ["baidu_integration_test"] * 2,
        "dag_id": ["dag_for_unittest"] * 4,
        "scene_id": ["20231220_1101", "underground_1220", "underground_1220", "20231220_1101"],
        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00", "fixed_a001", "fixed_a002", "manual__2023-12-21T02:53:04+00:00"]
    })

    expanded_df = await watcher.expand(upstream_df)

    gt = pd.DataFrame({
        "batch_id": ["baidu_integration_test"] * 5,
        "dag_id": ["dag_for_unittest"] * 5,
        "scene_id": ["20231220_1101"] * 5,
        "dag_run_id": ["manual__2023-12-21T02:53:04+00:00"] * 5,
        "split_id": [0, 1, 2, 3, 4],
    })
    gt.loc[:, "split_id"] = gt.split_id.astype("object")
    pd.testing.assert_frame_equal(expanded_df, gt)


@pytest.mark.asyncio
async def test_expand_when_xcom_dag_not_exist(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
        expand_by=XComQuery("dag_not_exist", "generate_split_map", "return_value", "split_id")
    )
    upstream_df = pd.DataFrame({
        "batch_id": ["doesnt_matter"] * 2 + ["baidu_integration_test"] * 2,
        "dag_id": ["dag_for_unittest"] * 4,
        "scene_id": ["20231220_1101", "underground_1220", "underground_1220", "20231220_1101"],
        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00", "fixed_a001", "fixed_a002", "manual__2023-12-21T02:53:04+00:00"]
    })

    expanded_df = await watcher.expand(upstream_df)
    assert isinstance(expanded_df, pd.DataFrame)
    assert len(expanded_df) == 0


@pytest.mark.asyncio
async def test_expand_when_xcom_task_not_success(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "an_interesting_batch_id",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "an_interesting_batch_id", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "an_interesting_batch_id", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
        expand_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )
    upstream_df = pd.DataFrame({
        "batch_id": ["doesnt_matter"] * 2 + ["an_interesting_batch_id"] * 2,
        "dag_id": ["dag_for_unittest"] * 4,
        "scene_id": ["20231220_1101", "underground_1220", "underground_1220", "20231220_1101"],
        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00", "fixed_a001", "fixed_a002", "manual__2023-12-21T02:53:04+00:00"]
    })

    expanded_df = await watcher.expand(upstream_df)

    assert isinstance(expanded_df, pd.DataFrame)
    assert len(expanded_df) == 0


@pytest.mark.asyncio
async def test_get_all_ready_scene(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert len(result) == 1
    assert result[0] == {"scene_id": "20231220_1101"}


@pytest.mark.asyncio
async def test_get_all_ready_scene_when_upstream_empty(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "batch_id_does_not_exist",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "batch_id_does_not_exist", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "batch_id_does_not_exist", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert result == []


@pytest.mark.asyncio
async def test_get_existing_scenes(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="dag_expandable",
        scene_id_keys=["scene_id"],
        expand_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "split_id": 1, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 0, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 2, "state": "failed"},
    ]


@pytest.mark.asyncio
async def test_get_existing_scenes_multi_scene_id_keys(cookies):
    watcher = ExpandableRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [],
        dag_id="dag_expandable",
        scene_id_keys=["scene_id", "split_id"],
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "split_id": 1, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 0, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 2, "state": "failed"},
    ]
