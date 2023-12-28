import pytest
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import ReducibleDagSensor
from scheduler.upstream_sensor.task_sensor import ReducibleTaskSensor


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_reduce_dag_sensor(cookies):
    sensor = ReducibleDagSensor(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_expandable",
        base_scene_id_keys=["scene_id"],
        reduce_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
    )
    reduced_df = await sensor.sense()
    assert len(reduced_df) == 1
    assert reduced_df.loc[0].scene_id == "20231220_1101"
    assert reduced_df.loc[0].batch_id == "baidu_integration_test"
    assert reduced_df.loc[0].dag_id == "dag_expandable"
    assert reduced_df.loc[0].dag_run_id == {"manual__2023-12-25T09:44:03+00:00", "20231220_1101_split_0", "20231220_1101_split_2", "20231220_1101_split_3", "20231220_1101_split_4"}
    assert reduced_df.loc[0].dag_run_state == {"success", "failed"}
    assert reduced_df.loc[0].state == "failed"


@pytest.mark.asyncio
async def test_reduce_dag_sensor_less_than_xcom_expands(cookies):
    sensor = ReducibleDagSensor(
        "http://127.0.0.1:8080",
        "a_new_batch_id",
        cookies,
        dag_id="dag_expandable",
        base_scene_id_keys=["scene_id"],
        reduce_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
    )
    reduced_df = await sensor.sense()
    assert len(reduced_df) == 1
    assert reduced_df.loc[0].scene_id == "underground_1220"
    assert reduced_df.loc[0].batch_id == {"a_new_batch_id", np.nan}
    assert reduced_df.loc[0].dag_id == {"dag_expandable", np.nan}
    assert reduced_df.loc[0].dag_run_id == {"runid_underground_0", "runid_underground_1", np.nan}
    assert reduced_df.loc[0].dag_run_state == {"success", np.nan}
    assert reduced_df.loc[0].state == "failed"


@pytest.mark.asyncio
async def test_reduce_task_sensor(cookies):
    sensor = ReducibleTaskSensor(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_expandable",
        task_id="hello",
        base_scene_id_keys=["scene_id"],
        reduce_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
    )
    reduced_df = await sensor.sense()
    assert len(reduced_df) == 1
    assert reduced_df.loc[0].scene_id == "20231220_1101"
    assert reduced_df.loc[0].batch_id == "baidu_integration_test"
    assert reduced_df.loc[0].dag_id == "dag_expandable"
    assert reduced_df.loc[0].task_id == "hello"
    assert reduced_df.loc[0].dag_run_id == {"manual__2023-12-25T09:44:03+00:00", "20231220_1101_split_0", "20231220_1101_split_2", "20231220_1101_split_3", "20231220_1101_split_4"}
    assert reduced_df.loc[0].dag_run_state == {"success", "failed"}
    assert reduced_df.loc[0].task_instance_state == "success"
    assert reduced_df.loc[0].state == "success"


@pytest.mark.asyncio
async def test_reduce_when_xcom_dag_not_exist(cookies):
    sensor = ReducibleDagSensor(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
        reduce_by={"dag_id": "dag_not_exist", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
    )

    reduced_df = await sensor.sense()
    assert isinstance(reduced_df, pd.DataFrame)
    assert len(reduced_df) == 0


@pytest.mark.asyncio
async def test_reduce_when_xcom_task_not_success(cookies):
    sensor = ReducibleDagSensor(
        "http://127.0.0.1:8080",
        "an_interesting_batch_id",
        cookies,
        dag_id="dag_for_unittest",
        base_scene_id_keys=["scene_id"],
        reduce_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
    )

    reduced_df = await sensor.sense()
    assert isinstance(reduced_df, pd.DataFrame)
    assert len(reduced_df) == 0
