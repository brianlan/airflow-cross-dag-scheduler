import pytest
import pandas as pd
import numpy as np
from unittest.mock import AsyncMock, patch

pd.set_option("display.max_columns", None)

from scheduler.watcher.restapi_watcher import RestAPIWatcher
from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_get_all_success_upstream_mixed(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        None,
        "downstream",
        {},
        [
            TaskSensor("http://127.0.0.1:8080", None, "dag_for_unittest", "fisheye.task_inside_2", cookies), 
            DagSensor("http://127.0.0.1:8080", None, "dag_for_unittest_another", cookies)
        ],
        ["scene_id"],
        "conf/cookie_session",
    )
    status_df = await watcher.get_all_success_upstream()
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.concat(
            [
                pd.DataFrame(
                    {
                        "dag_id": ["dag_for_unittest"] * 2,
                        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"]  + ["fixed_a001"] ,
                        "dag_run_state": ["success"] * 2,
                        "scene_id": ["20231220_1101"]  + ["underground_1220"] ,
                        "task_id": [ "fisheye.task_inside_2"] * 2,
                        "task_instance_state": ["success"] * 2,
                        "state": ["success"] * 2,
                    }
                ),
                pd.DataFrame(
                    {
                        "dag_id": ["dag_for_unittest_another"],
                        "dag_run_id": ["fixed_b001"],
                        "dag_run_state": ["success"],
                        "scene_id": ["20231220_1101"],
                        "task_id": [np.nan],
                        "task_instance_state": [np.nan],
                        "state": ["success"],
                    }
                ),
            ]
        ).reset_index(drop=True),
    )


@pytest.mark.asyncio
@patch("scheduler.watcher.restapi_watcher.RestAPIWatcher.get_all_success_upstream", new_callable=AsyncMock)
async def test_get_all_ready_scene(mock_get_all_upstream_status):
    mock_get_all_upstream_status.return_value = pd.concat(
        [
            pd.DataFrame(
                {
                    "batch_id": ["a_batch_id"] * 12,
                    "dag_id": ["dag_for_unittest"] * 12,
                    "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"] * 6 + ["fixed_a001"] * 6,
                    "dag_run_state": ["success"] * 12,
                    "scene_id": ["20231220_1101"] * 6 + ["underground_1220"] * 6,
                    "task_id": [
                        "final_task",
                        "fisheye.task_inside_1",
                        "fisheye.task_inside_2",
                        "generate_image",
                        "prepare_params",
                        "starting_task",
                    ]
                    * 2,
                    "task_instance_state": ["success"] * 12,
                    "state": ["success"] * 12,
                }
            ),
            pd.DataFrame(
                {
                    "batch_id": ["a_batch_id"],
                    "dag_id": ["dag_for_unittest_another"],
                    "dag_run_id": ["fixed_b001"],
                    "dag_run_state": ["success"],
                    "scene_id": ["20231220_1101"],
                    "task_id": [np.nan],
                    "task_instance_state": [np.nan],
                    "state": ["success"],
                }
            ),
        ]
    ).reset_index(drop=True)
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "a_batch_id",
        {},
        "downstream",
        [
            TaskSensor("http://127.0.0.1:8080", "a_batch_id", "dag_for_unittest", "fisheye.task_inside_2", cookies), 
            DagSensor("http://127.0.0.1:8080", "a_batch_id", "dag_for_unittest_another", cookies)
        ],
        ["scene_id"],
        "conf/cookie_session",
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert len(result) == 1
    assert result[0]["scene_id_keys"] == {"scene_id": "20231220_1101"}
    mock_get_all_upstream_status.assert_called_once()


@pytest.mark.asyncio
@patch("scheduler.watcher.restapi_watcher.RestAPIWatcher.get_all_success_upstream", new_callable=AsyncMock)
async def test_get_all_ready_scene_multi_scene_id_keys(mock_get_all_upstream_status):
    pass


@pytest.mark.asyncio
async def test_get_existing_scenes(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        "downstream",
        {},
        [
            TaskSensor("http://127.0.0.1:8080", None, "dag_for_unittest", "fisheye.task_inside_2", cookies), 
            DagSensor("http://127.0.0.1:8080", None, "dag_for_unittest_another", cookies)
        ],
        ["scene_id"],
        "conf/cookie_session",
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "state": "success"},
        {"scene_id": "underground_1220", "state": "failed"},
    ]
