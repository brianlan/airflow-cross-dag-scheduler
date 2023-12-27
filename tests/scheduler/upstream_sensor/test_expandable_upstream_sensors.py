import pytest
import pandas as pd

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import ExpandableDagSensor
from scheduler.upstream_sensor.task_sensor import ExpandableTaskSensor
from scheduler.upstream_sensor.xcom_query import XComQuery


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_expand_dag_sensor(cookies):
    sensor = ExpandableDagSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
    )
    expanded_df = await sensor.sense()
    gt = pd.DataFrame({
        "scene_id": ["20231220_1101"] * 5,
        "batch_id": ["baidu_integration_test"] * 5,
        "dag_id": ["dag_for_unittest"] * 5,
        "dag_run_id": ["manual__2023-12-21T02:53:04+00:00"] * 5,
        "dag_run_state": ["success"] * 5,
        "state": ["success"] * 5,
        "split_id": [0, 1, 2, 3, 4],
    })
    gt.loc[:, "split_id"] = gt.split_id.astype("object")
    pd.testing.assert_frame_equal(
        expanded_df[["scene_id", "batch_id", "dag_id", "dag_run_id", "dag_run_state", "state", "split_id"]], 
        gt
    )


@pytest.mark.asyncio
async def test_expand_task_sensor(cookies):
    sensor = ExpandableTaskSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_for_unittest",
        task_id="fisheye.task_inside_2",
        base_scene_id_keys=["scene_id"],
    )
    expanded_df = await sensor.sense()
    gt = pd.DataFrame({
        "scene_id": ["20231220_1101"] * 5,
        "batch_id": ["baidu_integration_test"] * 5,
        "dag_id": ["dag_for_unittest"] * 5,
        "task_id": ["fisheye.task_inside_2"] * 5,
        "dag_run_id": ["manual__2023-12-21T02:53:04+00:00"] * 5,
        "dag_run_state": ["success"] * 5,
        "task_instance_state": ["success"] * 5,
        "state": ["success"] * 5,
        "split_id": [0, 1, 2, 3, 4],
    })
    gt.loc[:, "split_id"] = gt.split_id.astype("object")
    pd.testing.assert_frame_equal(
        expanded_df[["scene_id", "batch_id", "dag_id", "task_id", "dag_run_id", "dag_run_state", "task_instance_state", "state", "split_id"]], 
        gt
    )


@pytest.mark.asyncio
async def test_expand_when_xcom_dag_not_exist(cookies):
    sensor = ExpandableDagSensor(
        XComQuery("dag_not_exist", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
    )

    expanded_df = await sensor.sense()
    assert isinstance(expanded_df, pd.DataFrame)
    assert len(expanded_df) == 0


@pytest.mark.asyncio
async def test_expand_when_xcom_task_not_success(cookies):
    sensor = ExpandableDagSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "an_interesting_batch_id",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
    )

    expanded_df = await sensor.sense()
    assert isinstance(expanded_df, pd.DataFrame)
    assert len(expanded_df) == 0
