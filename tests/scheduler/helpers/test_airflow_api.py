import pytest
import pandas as pd

from scheduler.helpers.airflow_api import get_dag_runs, get_task_instance


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}

@pytest.mark.asyncio
async def test_get_dag_runs(cookies):
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", None, "dag_for_unittest", cookies, to_dataframe=False)
    assert len(dag_runs) == 2
    assert {d["dag_run_id"] for d in dag_runs} == {"manual__2023-12-20T03:38:06+00:00", "fixed_a001"}


@pytest.mark.asyncio
async def test_get_dag_runs_with_batch_id(cookies):
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_for_unittest", cookies, to_dataframe=False)
    assert len(dag_runs) == 2
    assert {d["dag_run_id"] for d in dag_runs} == {"manual__2023-12-21T02:53:04+00:00", "fixed_a002"}


@pytest.mark.asyncio
async def test_get_dag_runs_with_no_dagrun(cookies):
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_has_no_dagrun", cookies, to_dataframe=False)
    assert dag_runs == []
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_has_no_dagrun", cookies, to_dataframe=True)
    assert isinstance(dag_runs, pd.DataFrame)
    assert len(dag_runs) == 0


@pytest.mark.asyncio
async def test_get_dag_runs_with_no_dag_id(cookies):
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_id_does_not_exist", cookies, to_dataframe=False)
    assert dag_runs == []
    dag_runs = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_id_does_not_exist", cookies, to_dataframe=True)
    assert isinstance(dag_runs, pd.DataFrame)
    assert len(dag_runs) == 0


@pytest.mark.asyncio
async def test_get_task_instance(cookies):
    ti = await get_task_instance("http://127.0.0.1:8080", "dag_for_unittest", "fixed_a001", "final_task", cookies, to_dataframe=False)
    assert ti == [{
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
    }]


# @pytest.mark.asyncio
# async def test_get_task_instances(cookies):
#     task_instances = await get_task_instances("http://127.0.0.1:8080", "dag_for_unittest", "fixed_a001", cookies, to_dataframe=False)
#     assert task_instances == [
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 0.124446,
#             "end_date": "2023-12-20T10:14:27.561202+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "a5a856482abf",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "_PythonDecoratedOperator",
#             "pid": 49159,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 1,
#             "queue": "default",
#             "queued_when": "2023-12-20T10:14:26.996557+00:00",
#             "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:27.436756+00:00",
#             "state": "success",
#             "task_id": "final_task",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 1,
#             "unixname": "default",
#         },
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 0.0,
#             "end_date": "2023-12-20T10:14:24.865733+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "EmptyOperator",
#             "pid": None,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 3,
#             "queue": "default",
#             "queued_when": None,
#             "rendered_fields": {},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:24.865725+00:00",
#             "state": "success",
#             "task_id": "fisheye.task_inside_1",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 0,
#             "unixname": "default",
#         },
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 0.0,
#             "end_date": "2023-12-20T10:14:25.933345+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "EmptyOperator",
#             "pid": None,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 2,
#             "queue": "default",
#             "queued_when": None,
#             "rendered_fields": {},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:25.933340+00:00",
#             "state": "success",
#             "task_id": "fisheye.task_inside_2",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 0,
#             "unixname": "default",
#         },
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 5.254295,
#             "end_date": "2023-12-20T10:14:24.383720+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "a5a856482abf",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "_PythonDecoratedOperator",
#             "pid": 49157,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 4,
#             "queue": "default",
#             "queued_when": "2023-12-20T10:14:18.764445+00:00",
#             "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:19.129425+00:00",
#             "state": "success",
#             "task_id": "generate_image",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 1,
#             "unixname": "default",
#         },
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 0.3311,
#             "end_date": "2023-12-20T10:14:17.615567+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "a5a856482abf",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "_PythonDecoratedOperator",
#             "pid": 49153,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 6,
#             "queue": "default",
#             "queued_when": "2023-12-20T10:14:15.879295+00:00",
#             "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:17.284467+00:00",
#             "state": "success",
#             "task_id": "prepare_params",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 1,
#             "unixname": "default",
#         },
#         {
#             "dag_id": "dag_for_unittest",
#             "dag_run_id": "fixed_a001",
#             "duration": 0.152061,
#             "end_date": "2023-12-20T10:14:18.541004+00:00",
#             "execution_date": "2023-12-20T10:13:35+00:00",
#             "executor_config": "{}",
#             "hostname": "a5a856482abf",
#             "map_index": -1,
#             "max_tries": 3,
#             "note": None,
#             "operator": "_PythonDecoratedOperator",
#             "pid": 49155,
#             "pool": "default_pool",
#             "pool_slots": 1,
#             "priority_weight": 5,
#             "queue": "default",
#             "queued_when": "2023-12-20T10:14:18.071387+00:00",
#             "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
#             "sla_miss": None,
#             "start_date": "2023-12-20T10:14:18.388943+00:00",
#             "state": "success",
#             "task_id": "starting_task",
#             "trigger": None,
#             "triggerer_job": None,
#             "try_number": 1,
#             "unixname": "default",
#         },
#     ]


@pytest.mark.asyncio
async def test_get_dag_runs_with_flatten_conf(cookies):
    dag_runs_df = await get_dag_runs("http://127.0.0.1:8080", "baidu_integration_test", "dag_for_unittest", cookies, to_dataframe=True, flatten_conf=True)
    pd.testing.assert_frame_equal(
        dag_runs_df[["dag_id", "dag_run_id", "state", "dag_run_state", "batch_id", "scene_id"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_for_unittest"] * 2,
                "dag_run_id": ["fixed_a002", "manual__2023-12-21T02:53:04+00:00"] ,
                "state": ["success"] * 2,
                "dag_run_state": ["success"] * 2,
                "batch_id": ["baidu_integration_test"] * 2 ,
                "scene_id": ["underground_1220", "20231220_1101"] ,
            }
        )
    )