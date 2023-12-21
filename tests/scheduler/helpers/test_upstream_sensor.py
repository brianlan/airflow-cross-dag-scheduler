import pytest
import pandas as pd

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor
from scheduler.upstream_sensor.static_scene_list_sensor import StaticSceneListSensor


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}

@pytest.mark.asyncio
async def test_dag_sensor(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", None, "dag_for_unittest", cookies)
    status_df = await sensor.sense()
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "state"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_for_unittest"] * 2,
                "dag_run_id": ["manual__2023-12-20T03:38:06+00:00", "fixed_a001"],
                "dag_run_state": ["success"] * 2,
                "scene_id": ["20231220_1101", "underground_1220"],
                "state": ["success"] * 2,
            }
        ),
    )


@pytest.mark.asyncio
async def test_dag_sensor_with_state_success(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", "baidu_integration_test", "downstream", cookies)
    status_df = await sensor.sense(state="success")
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["batch_id", "dag_id", "dag_run_id", "dag_run_state", "scene_id", "state"]],
        pd.DataFrame(
            {
                "batch_id": ["baidu_integration_test"],
                "dag_id": ["downstream"],
                "dag_run_id": ["fixed_d001"],
                "dag_run_state": ["success"],
                "scene_id": ["20231220_1101"],
                "state": ["success"],
            }
        ),
    )



@pytest.mark.asyncio
async def test_task_sensor(cookies):
    sensor = TaskSensor("http://127.0.0.1:8080", None, "dag_for_unittest", "fisheye.task_inside_2", cookies)
    status_df = await sensor.sense()
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_for_unittest"] * 2,
                "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"] + ["fixed_a001"],
                "dag_run_state": ["success"] * 2,
                "scene_id": ["20231220_1101"] + ["underground_1220"],
                "task_id": ["fisheye.task_inside_2"] * 2,
                "task_instance_state": ["success"] * 2,
                "state": ["success"] * 2,
            }
        ),
    )


@pytest.mark.asyncio
async def test_task_sensor_with_state_failed(cookies):
    sensor = TaskSensor("http://127.0.0.1:8080", "not_tracked", "downstream", "random_fail", cookies)
    status_df = await sensor.sense(state="failed")
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["batch_id", "dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.DataFrame(
            {
                "batch_id": ["not_tracked"],
                "dag_id": ["downstream"],
                "dag_run_id": ["scene_id:20231220_7777"],
                "dag_run_state": ["failed"],
                "scene_id": ["20231220_7777"],
                "task_id": ["random_fail"],
                "task_instance_state": ["failed"],
                "state": ["failed"],
            }
        ),
    )


@pytest.mark.asyncio
async def test_static_scene_list_sensor():
    sensor = StaticSceneListSensor("a_batch_id", [{"scene_id": "scn_001"}, {"scene_id": "scn_002"}])
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame(
            {
                "scene_id": ["scn_001", "scn_002"],
                "batch_id": ["a_batch_id"] * 2,
                "state": ["success"] * 2,
            }
        )
    )