import pytest
import pandas as pd
import numpy as np
from unittest.mock import AsyncMock, patch

pd.set_option("display.max_columns", None)

from scheduler.watcher.restapi_watcher import RestAPIWatcher


@pytest.mark.asyncio
async def test_get_dag_runs():
    watcher = RestAPIWatcher("http://127.0.0.1:8080", None, "downstream", {}, [], ["scene_id"], "conf/cookie_session")
    dag_runs = await watcher.get_dag_runs("dag_for_unittest", False)
    assert len(dag_runs) == 2
    assert {d["dag_run_id"] for d in dag_runs} == {"manual__2023-12-20T03:38:06+00:00", "fixed_a001"}


@pytest.mark.asyncio
async def test_get_dag_runs_with_batch_id():
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080", "baidu_integration_test", "downstream", {}, [], ["scene_id"], "conf/cookie_session"
    )
    dag_runs = await watcher.get_dag_runs("dag_for_unittest", False)
    assert len(dag_runs) == 2
    assert {d["dag_run_id"] for d in dag_runs} == {"manual__2023-12-21T02:53:04+00:00", "fixed_a002"}


@pytest.mark.asyncio
async def test_get_taskinstances():
    watcher = RestAPIWatcher("http://127.0.0.1:8080", None, "downstream", {}, [], ["scene_id"], "conf/cookie_session")
    taskinstances = await watcher.get_taskinstances("dag_for_unittest", "fixed_a001")
    assert taskinstances == [
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 0.124446,
            "end_date": "2023-12-20T10:14:27.561202+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "a5a856482abf",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "_PythonDecoratedOperator",
            "pid": 49159,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": "2023-12-20T10:14:26.996557+00:00",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:27.436756+00:00",
            "state": "success",
            "task_id": "final_task",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "default",
        },
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 0.0,
            "end_date": "2023-12-20T10:14:24.865733+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "EmptyOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 3,
            "queue": "default",
            "queued_when": None,
            "rendered_fields": {},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:24.865725+00:00",
            "state": "success",
            "task_id": "fisheye.task_inside_1",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 0,
            "unixname": "default",
        },
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 0.0,
            "end_date": "2023-12-20T10:14:25.933345+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "EmptyOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 2,
            "queue": "default",
            "queued_when": None,
            "rendered_fields": {},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:25.933340+00:00",
            "state": "success",
            "task_id": "fisheye.task_inside_2",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 0,
            "unixname": "default",
        },
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 5.254295,
            "end_date": "2023-12-20T10:14:24.383720+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "a5a856482abf",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "_PythonDecoratedOperator",
            "pid": 49157,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 4,
            "queue": "default",
            "queued_when": "2023-12-20T10:14:18.764445+00:00",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:19.129425+00:00",
            "state": "success",
            "task_id": "generate_image",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "default",
        },
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 0.3311,
            "end_date": "2023-12-20T10:14:17.615567+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "a5a856482abf",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "_PythonDecoratedOperator",
            "pid": 49153,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 6,
            "queue": "default",
            "queued_when": "2023-12-20T10:14:15.879295+00:00",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:17.284467+00:00",
            "state": "success",
            "task_id": "prepare_params",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "default",
        },
        {
            "dag_id": "dag_for_unittest",
            "dag_run_id": "fixed_a001",
            "duration": 0.152061,
            "end_date": "2023-12-20T10:14:18.541004+00:00",
            "execution_date": "2023-12-20T10:13:35+00:00",
            "executor_config": "{}",
            "hostname": "a5a856482abf",
            "map_index": -1,
            "max_tries": 3,
            "note": None,
            "operator": "_PythonDecoratedOperator",
            "pid": 49155,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 5,
            "queue": "default",
            "queued_when": "2023-12-20T10:14:18.071387+00:00",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "sla_miss": None,
            "start_date": "2023-12-20T10:14:18.388943+00:00",
            "state": "success",
            "task_id": "starting_task",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "default",
        },
    ]


@pytest.mark.asyncio
async def test_get_all_upstream_status_only_dag():
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        None,
        "downstream",
        {},
        [{"dag_id": "dag_for_unittest"}],
        ["scene_id"],
        "conf/cookie_session",
    )
    status_df = await watcher.get_all_upstream_status()
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
async def test_get_all_upstream_status_with_task_instances():
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        None,
        "downstream",
        {},
        [{"dag_id": "dag_for_unittest", "task_id": "fisheye.task_inside_2"}],
        ["scene_id"],
        "conf/cookie_session",
    )
    status_df = await watcher.get_all_upstream_status()
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.DataFrame(
            {
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
    )


@pytest.mark.asyncio
async def test_get_all_upstream_status_mixed():
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        None,
        "downstream",
        {},
        [{"dag_id": "dag_for_unittest", "task_id": "fisheye.task_inside_2"}, {"dag_id": "dag_for_unittest_another"}],
        ["scene_id"],
        "conf/cookie_session",
    )
    status_df = await watcher.get_all_upstream_status()
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.concat(
            [
                pd.DataFrame(
                    {
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
@patch("scheduler.watcher.restapi_watcher.RestAPIWatcher.get_all_upstream_status", new_callable=AsyncMock)
async def test_get_all_ready_scene(mock_get_all_upstream_status):
    mock_get_all_upstream_status.return_value = pd.concat(
        [
            pd.DataFrame(
                {
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
        None,
        {},
        "downstream",
        [{"dag_id": "dag_for_unittest", "task_id": "fisheye.task_inside_2"}, {"dag_id": "dag_for_unittest_another"}],
        ["scene_id"],
        "conf/cookie_session",
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert len(result) == 1
    assert result[0]["scene_id_keys"] == {"scene_id": "20231220_1101"}
    mock_get_all_upstream_status.assert_called_once()


@pytest.mark.asyncio
@patch("scheduler.watcher.restapi_watcher.RestAPIWatcher.get_all_upstream_status", new_callable=AsyncMock)
async def test_get_all_ready_scene_multi_scene_id_keys(mock_get_all_upstream_status):
    pass


@pytest.mark.asyncio
async def test_get_existing_scenes():
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        "downstream",
        {},
        [{"dag_id": "dag_for_unittest", "task_id": "fisheye.task_inside_2"}, {"dag_id": "dag_for_unittest_another"}],
        ["scene_id"],
        "conf/cookie_session",
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "state": "success"},
        {"scene_id": "underground_1220", "state": "failed"},
    ]
