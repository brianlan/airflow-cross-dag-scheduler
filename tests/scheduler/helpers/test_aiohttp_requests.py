import pytest
import aiohttp

from scheduler.helpers import aiohttp_requests as ar
from scheduler.helpers.aiohttp_requests import async_retry
from scheduler.helpers.base import async_read_cookie_session


@pytest.mark.asyncio
async def test_get_dag_id():
    cookies = {"session": await async_read_cookie_session("conf/cookie_session")}
    status, result = await ar.get("http://127.0.0.1:8080/api/v1/dags/dag_for_unittest", cookies=cookies)
    result.pop("last_parsed_time")
    assert status == 200
    assert result == {
        "dag_id": "dag_for_unittest",
        "default_view": "grid",
        "description": "dag_for_unittest",
        "file_token": "Ii9vcHQvYWlyZmxvdy9kYWdzL2RhZ19mb3JfdW5pdHRlc3QucHki.wCVxNK7N2azAiOJBeOOVIcAuB_Y",
        "fileloc": "/opt/airflow/dags/dag_for_unittest.py",
        "has_import_errors": False,
        "has_task_concurrency_limits": False,
        "is_active": True,
        "is_paused": False,
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
        "tags": [],
        "timetable_description": "Never, external triggers only",
    }


# Mock function for testing
async def mock_function(should_fail_times):
    if mock_function.counter < should_fail_times:
        mock_function.counter += 1
        raise aiohttp.ClientError("Mocked client error")
    return 200, "Success"

# Apply the decorator (now the new mock_function is the decorated version with retry logic)
mock_function_with_retry = async_retry(retries=3, delay=0.1)(mock_function)

@pytest.mark.asyncio
async def test_retry_success():
    mock_function.counter = 0
    status, message = await mock_function_with_retry(2)  # should fail 2 times, then succeed
    assert status == 200 and message == "Success"

@pytest.mark.asyncio
async def test_retry_failure():
    mock_function.counter = 0
    with pytest.raises(aiohttp.ClientError):
        await mock_function_with_retry(4)  # should fail all 4 times

@pytest.mark.asyncio
async def test_no_retry_needed():
    mock_function.counter = 0
    status, message = await mock_function_with_retry(0)  # should succeed on first attempt
    assert status == 200 and message == "Success"
