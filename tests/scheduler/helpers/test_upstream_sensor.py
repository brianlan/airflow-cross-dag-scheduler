import pytest
import pandas as pd

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}

@pytest.mark.asyncio
async def test_get_all_upstream_status_only_dag(cookies):
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
async def test_get_all_upstream_status_with_task_instances(cookies):
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
