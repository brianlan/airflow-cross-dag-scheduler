import pytest

# import aiohttp
# from unittest.mock import patch, MagicMock
from scheduler.watcher.base_watcher import RestAPIWatcher  # Import your class


@pytest.mark.asyncio
async def test_get_dagruns():
    watcher = RestAPIWatcher("http://127.0.0.1:8080", "dag_for_unittest", [], "conf/cookie_session")
    dagruns = await watcher.get_dagruns("dag_for_unittest", False)
    assert len(dagruns) == 2
    assert {d["dag_run_id"] for d in dagruns} == {"manual__2023-12-20T03:38:06+00:00", "fixed_a001"}


@pytest.mark.asyncio
async def test_get_dagruns_with_taskinstances():
    watcher = RestAPIWatcher("http://127.0.0.1:8080", "dag_for_unittest", [], "conf/cookie_session")
    dagruns = await watcher.get_dagruns("dag_for_unittest", True)
    fixed_a001 = next(d for d in dagruns if d["dag_run_id"] == "fixed_a001")
    assert fixed_a001 == {
        "conf": {"scene_id": "20231220_1101"},
        "dag_id": "dag_for_unittest",
        "dag_run_id": "fixed_a001",
        "data_interval_end": "2023-12-20T04:45:31+00:00",
        "data_interval_start": "2023-12-20T04:45:31+00:00",
        "end_date": "2023-12-20T04:46:18.111282+00:00",
        "execution_date": "2023-12-20T04:45:31+00:00",
        "external_trigger": True,
        "last_scheduling_decision": "2023-12-20T04:46:18.109161+00:00",
        "logical_date": "2023-12-20T04:45:31+00:00",
        "note": None,
        "run_type": "manual",
        "start_date": "2023-12-20T04:46:05.584199+00:00",
        "state": "success",
        "task_instances": [
            {
                "dag_id": "dag_for_unittest",
                "dag_run_id": "fixed_a001",
                "duration": 0.159978,
                "end_date": "2023-12-20T04:46:17.662767+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
                "executor_config": "{}",
                "hostname": "a5a856482abf",
                "map_index": -1,
                "max_tries": 3,
                "note": None,
                "operator": "_PythonDecoratedOperator",
                "pid": 26110,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 1,
                "queue": "default",
                "queued_when": "2023-12-20T04:46:16.977275+00:00",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "sla_miss": None,
                "start_date": "2023-12-20T04:46:17.502789+00:00",
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
                "end_date": "2023-12-20T04:46:14.877379+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
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
                "start_date": "2023-12-20T04:46:14.877375+00:00",
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
                "end_date": "2023-12-20T04:46:15.920728+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
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
                "start_date": "2023-12-20T04:46:15.920724+00:00",
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
                "duration": 5.384514,
                "end_date": "2023-12-20T04:46:13.788597+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
                "executor_config": "{}",
                "hostname": "a5a856482abf",
                "map_index": -1,
                "max_tries": 3,
                "note": None,
                "operator": "_PythonDecoratedOperator",
                "pid": 26091,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 4,
                "queue": "default",
                "queued_when": "2023-12-20T04:46:08.059856+00:00",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "sla_miss": None,
                "start_date": "2023-12-20T04:46:08.404083+00:00",
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
                "duration": 0.198325,
                "end_date": "2023-12-20T04:46:06.726291+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
                "executor_config": "{}",
                "hostname": "a5a856482abf",
                "map_index": -1,
                "max_tries": 3,
                "note": None,
                "operator": "_PythonDecoratedOperator",
                "pid": 26087,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 6,
                "queue": "default",
                "queued_when": "2023-12-20T04:46:05.653539+00:00",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "sla_miss": None,
                "start_date": "2023-12-20T04:46:06.527966+00:00",
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
                "duration": 0.16716,
                "end_date": "2023-12-20T04:46:07.713116+00:00",
                "execution_date": "2023-12-20T04:45:31+00:00",
                "executor_config": "{}",
                "hostname": "a5a856482abf",
                "map_index": -1,
                "max_tries": 3,
                "note": None,
                "operator": "_PythonDecoratedOperator",
                "pid": 26089,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 5,
                "queue": "default",
                "queued_when": "2023-12-20T04:46:06.967924+00:00",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "sla_miss": None,
                "start_date": "2023-12-20T04:46:07.545956+00:00",
                "state": "success",
                "task_id": "starting_task",
                "trigger": None,
                "triggerer_job": None,
                "try_number": 1,
                "unixname": "default",
            },
        ],
    }


# @pytest.mark.asyncio
# async def test_get_taskinstances_success():
#     # Mock response
#     response_mock = MagicMock()
#     response_mock.status = 200
#     response_mock.json = MagicMock(return_value={"task_instances": [{"id": 1, "state": "success"}]})

#     # Mock session.get to return the mock response
#     session_mock = MagicMock()
#     session_mock.get = MagicMock(return_value=response_mock)

#     # Mock aiohttp.ClientSession to use our mock session
#     with patch("aiohttp.ClientSession", return_value=session_mock):
#         watcher = RestAPIWatcher("http://mock-api.com", "mock_dag", [])
#         result = await watcher.get_taskinstances("mock_dag", "mock_run")

#         assert result == [{"id": 1, "state": "success"}]


# @pytest.mark.asyncio
# async def test_get_taskinstances_failure():
#     # Mock a failure response
#     response_mock = MagicMock()
#     response_mock.status = 404  # Not found, or other HTTP error status

#     session_mock = MagicMock()
#     session_mock.get = MagicMock(return_value=response_mock)

#     with patch("aiohttp.ClientSession", return_value=session_mock):
#         watcher = RestAPIWatcher("http://mock-api.com", "mock_dag", [])
#         result = await watcher.get_taskinstances("mock_dag", "mock_run")

#         assert result == []  # Empty list for failure cases
