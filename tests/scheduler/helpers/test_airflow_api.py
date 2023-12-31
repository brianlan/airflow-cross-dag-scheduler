import pytest
import pandas as pd

from scheduler.helpers.airflow_api import get_dag_runs, get_task_instance, get_xcom, get_dag_info, trigger_dag
from scheduler.helpers.aiohttp_requests import Non200Response


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
    dag_runs = await get_dag_runs(
        "http://127.0.0.1:8080", "baidu_integration_test", "dag_for_unittest", cookies, to_dataframe=False
    )
    assert len(dag_runs) == 2
    assert {d["dag_run_id"] for d in dag_runs} == {"manual__2023-12-21T02:53:04+00:00", "fixed_a002"}


@pytest.mark.asyncio
async def test_get_dag_runs_with_no_dagrun(cookies):
    dag_runs = await get_dag_runs(
        "http://127.0.0.1:8080", "baidu_integration_test", "dag_has_no_dagrun", cookies, to_dataframe=False
    )
    assert dag_runs == []
    dag_runs = await get_dag_runs(
        "http://127.0.0.1:8080", "baidu_integration_test", "dag_has_no_dagrun", cookies, to_dataframe=True
    )
    assert isinstance(dag_runs, pd.DataFrame)
    assert len(dag_runs) == 0


@pytest.mark.asyncio
async def test_get_dag_runs_with_no_dag_id(cookies):
    dag_runs = await get_dag_runs(
        "http://127.0.0.1:8080", "baidu_integration_test", "dag_id_does_not_exist", cookies, to_dataframe=False
    )
    assert dag_runs == []
    dag_runs = await get_dag_runs(
        "http://127.0.0.1:8080", "baidu_integration_test", "dag_id_does_not_exist", cookies, to_dataframe=True
    )
    assert isinstance(dag_runs, pd.DataFrame)
    assert len(dag_runs) == 0


@pytest.fixture
def gt_task_instance():
    return {
        "dag_id": "dag_for_unittest",
        "dag_run_id": "fixed_a001",
        "duration": 0.124446,
        "end_date": "2023-12-20T10:14:27.561202+00:00",
        "execution_date": "2023-12-20T10:13:35+00:00",
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
        "sla_miss": None,
        "start_date": "2023-12-20T10:14:27.436756+00:00",
        "state": "success",
        "task_id": "final_task",
        "trigger": None,
        "triggerer_job": None,
        "try_number": 1,
        "unixname": "default",
        "task_instance_state": "success",
    }


@pytest.mark.asyncio
async def test_get_task_instance(cookies, gt_task_instance):
    ti = await get_task_instance(
        "http://127.0.0.1:8080", "dag_for_unittest", "fixed_a001", "final_task", cookies, to_dataframe=False
    )
    assert ti == gt_task_instance
    


@pytest.mark.asyncio
async def test_get_task_instance_to_df(cookies, gt_task_instance):
    ti_df = await get_task_instance(
        "http://127.0.0.1:8080", "dag_for_unittest", "fixed_a001", "final_task", cookies, to_dataframe=True
    )
    pd.testing.assert_frame_equal( ti_df, pd.DataFrame.from_records([gt_task_instance]) )


@pytest.mark.asyncio
async def test_get_task_instance_when_ti_not_exist(cookies):
    with pytest.raises(Non200Response):
        _ = await get_task_instance(
            "http://127.0.0.1:8080", "dag_run_without_task", "to_remove", "task_xxx", cookies, to_dataframe=False
        )


@pytest.mark.asyncio
async def test_get_dag_runs_with_flatten_conf(cookies):
    dag_runs_df = await get_dag_runs(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        "dag_for_unittest",
        cookies,
        to_dataframe=True,
        flatten_conf=True,
    )
    pd.testing.assert_frame_equal(
        dag_runs_df[["dag_id", "dag_run_id", "state", "dag_run_state", "batch_id", "scene_id"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_for_unittest"] * 2,
                "dag_run_id": ["fixed_a002", "manual__2023-12-21T02:53:04+00:00"],
                "state": ["success"] * 2,
                "dag_run_state": ["success"] * 2,
                "batch_id": ["baidu_integration_test"] * 2,
                "scene_id": ["underground_1220", "20231220_1101"],
            }
        ),
    )


@pytest.mark.asyncio
async def test_get_xcom(cookies):
    result = await get_xcom(
        "http://127.0.0.1:8080",
        "dag_split_map_generator",
        "manual__2023-12-25T10:27:12+00:00",
        "generate_split_map",
        cookies,
        to_dataframe=False,
    )
    assert (
        result[0]["value"]
        == "[{'S3_SPLIT_MAP_ID': 0}, {'S3_SPLIT_MAP_ID': 1}, {'S3_SPLIT_MAP_ID': 2}, {'S3_SPLIT_MAP_ID': 3}, {'S3_SPLIT_MAP_ID': 4}]"
    )
    result_df = await get_xcom(
        "http://127.0.0.1:8080",
        "dag_split_map_generator",
        "manual__2023-12-25T10:27:12+00:00",
        "generate_split_map",
        cookies,
        to_dataframe=True,
    )
    pd.testing.assert_frame_equal(
        result_df[["dag_id", "key", "task_id", "value"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_split_map_generator"],
                "key": ["return_value"],
                "task_id": ["generate_split_map"],
                "value": [
                    "[{'S3_SPLIT_MAP_ID': 0}, {'S3_SPLIT_MAP_ID': 1}, {'S3_SPLIT_MAP_ID': 2}, {'S3_SPLIT_MAP_ID': 3}, {'S3_SPLIT_MAP_ID': 4}]"
                ],
            }
        ),
    )


@pytest.mark.asyncio
async def test_get_xcom_specify_xcom_key(cookies):
    assert (
        await get_xcom(
            "http://127.0.0.1:8080",
            "dag_split_map_generator",
            "manual__2023-12-25T10:27:12+00:00",
            "generate_xcom_key_value_pairs",
            cookies,
            to_dataframe=False,
            xcom_key="return_value",
        )
    )[0]["value"] == "{'p': 'past', 'f': 'future'}"

    assert (
        await get_xcom(
            "http://127.0.0.1:8080",
            "dag_split_map_generator",
            "manual__2023-12-25T10:27:12+00:00",
            "generate_xcom_key_value_pairs",
            cookies,
            to_dataframe=False,
            xcom_key="p",
        )
    )[0]["value"] == "past"

    assert (
        await get_xcom(
            "http://127.0.0.1:8080",
            "dag_split_map_generator",
            "manual__2023-12-25T10:27:12+00:00",
            "generate_xcom_key_value_pairs",
            cookies,
            to_dataframe=False,
            xcom_key="f",
        )
    )[0]["value"] == "future"


@pytest.mark.asyncio
async def test_get_xcom_task_instance_not_exist(cookies):
    with pytest.raises(Non200Response):
        _ = await get_xcom(
            "http://127.0.0.1:8080",
            "dag_split_map_generator",
            "manual__2023-12-25T10:27:12+00:00",
            "task_id_not_exist",
            cookies,
            to_dataframe=False,
            xcom_key="f",
        )


@pytest.mark.asyncio
async def test_get_dag_info(cookies):
    dag_info = await get_dag_info(
        "http://127.0.0.1:8080",
        "dag_paused",
        cookies,
    )
    dag_info.pop("file_token")
    dag_info.pop("last_parsed_time")
    assert dag_info == {
        "dag_id": "dag_paused",
        "default_view": "grid",
        "description": "dag_paused",
        "fileloc": "/opt/airflow/dags/dag_paused.py",
        "has_import_errors": False,
        "has_task_concurrency_limits": False,
        "is_active": True,
        "is_paused": True,
        "is_subdag": False,
        "last_expired": None,
        "last_pickled": None,
        "max_active_runs": 16,
        "max_active_tasks": 16,
        "next_dagrun": None,
        "next_dagrun_create_after": None,
        "next_dagrun_data_interval_end": None,
        "next_dagrun_data_interval_start": None,
        "owners": ["airflow"],
        "pickle_id": None,
        "root_dag_id": None,
        "schedule_interval": None,
        "scheduler_lock": None,
        "tags": [{"name": "unit-testing"}],
        "timetable_description": "Never, external triggers only",
    }


@pytest.mark.asyncio
async def test_trigger_dag_when_dag_is_paused(cookies):
    status, dag_info = await trigger_dag(
        "http://127.0.0.1:8080",
        "dag_paused",
        cookies,
    )
    assert status == 200
    assert "message" in dag_info
    assert "paused" in dag_info["message"]
