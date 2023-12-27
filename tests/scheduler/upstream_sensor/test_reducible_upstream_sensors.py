import pytest
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import ReducibleDagSensor
from scheduler.upstream_sensor.task_sensor import ReducibleTaskSensor
from scheduler.upstream_sensor.xcom_query import XComQuery


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_reduce_dag_sensor(cookies):
    sensor = ReducibleDagSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_expandable",
        base_scene_id_keys=["scene_id"],
    )
    reduced_df = await sensor.sense()
    gt = pd.DataFrame({
        "scene_id": ["20231220_1101"],
        "batch_id": [["baidu_integration_test", "baidu_integration_test", "baidu_integration_test", np.nan, np.nan]],
        "dag_id": [["dag_expandable", "dag_expandable", "dag_expandable", np.nan, np.nan]],
        "dag_run_id": [["manual__2023-12-25T09:44:03+00:00", "20231220_1101_split_0", "20231220_1101_split_2", np.nan, np.nan]],
        "dag_run_state": [["success", "success", "failed", np.nan, np.nan]],
        "state": ["failed"],
    })
    pd.testing.assert_frame_equal(
        reduced_df[["scene_id", "batch_id", "dag_id", "dag_run_id", "dag_run_state", "state"]], 
        gt
    )


@pytest.mark.asyncio
async def test_reduce_task_sensor(cookies):
    sensor = ReducibleTaskSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_expandable",
        task_id="hello",
        base_scene_id_keys=["scene_id"],
    )
    reduced_df = await sensor.sense()
    gt = pd.DataFrame({
        "scene_id": ["20231220_1101"],
        "batch_id": [["baidu_integration_test"] * 5],
        "dag_id": [["dag_expandable"] * 5],
        "task_id": [["hello"] * 5],
        "dag_run_id": [["manual__2023-12-25T09:44:03+00:00", "20231220_1101_split_0", "20231220_1101_split_2", "20231220_1101_split_3", "20231220_1101_split_4"]],
        "dag_run_state": [["success", "success", "failed", "success", "success"]],
        "task_instance_state": [["success"] * 5],
        "state": ["success"],
    })
    pd.testing.assert_frame_equal(
        reduced_df[["scene_id", "batch_id", "dag_id", "task_id", "dag_run_id", "dag_run_state", "task_instance_state", "state"]], 
        gt
    )


@pytest.mark.asyncio
async def test_reduce_when_xcom_dag_not_exist(cookies):
    sensor = ReducibleDagSensor(
        XComQuery("dag_not_exist", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
    )

    reduced_df = await sensor.sense()
    assert isinstance(reduced_df, pd.DataFrame)
    assert len(reduced_df) == 0


@pytest.mark.asyncio
async def test_reduce_when_xcom_task_not_success(cookies):
    sensor = ReducibleDagSensor(
        XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id"),
        "http://127.0.0.1:8080",
        "an_interesting_batch_id",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
    )

    reduced_df = await sensor.sense()
    assert isinstance(reduced_df, pd.DataFrame)
    assert len(reduced_df) == 0
