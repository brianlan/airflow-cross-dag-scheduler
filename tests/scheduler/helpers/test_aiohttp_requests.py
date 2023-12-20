import pytest

from scheduler.helpers import aiohttp_requests as aio
from scheduler.helpers.base import async_read_cookie_session


@pytest.mark.asyncio
async def test_get_dag_id():
    cookies = {"session": await async_read_cookie_session("conf/cookie_session")}
    result = await aio.get("http://127.0.0.1:8080/api/v1/dags/dag_for_unittest", cookies=cookies)
    result.pop("last_parsed_time")
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
