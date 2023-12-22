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
async def test_get_all_ready_scene(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert len(result) == 1
    assert result[0] == {"scene_id": "20231220_1101"}


@pytest.mark.asyncio
async def test_get_all_ready_scene_when_upstream_empty(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "batch_id_does_not_exist",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", "batch_id_does_not_exist", cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", "batch_id_does_not_exist", cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert result == []


@pytest.mark.asyncio
async def test_get_existing_scenes(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            TaskSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"), 
            DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest_another")
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id"],
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "state": "success"},
        {"scene_id": "underground_1220", "state": "failed"},
    ]
