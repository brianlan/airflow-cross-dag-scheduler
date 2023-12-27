import pytest
import pandas as pd

pd.set_option("display.max_columns", None)

from scheduler.watcher.reducible_restapi_watcher import ReducibleRestAPIWatcher
from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor
from scheduler.upstream_sensor.xcom_query import XComQuery


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_reduced_scene_id_keys(cookies):
    watcher = ReducibleRestAPIWatcher(
        "http://127.0.0.1:8080",
        "whatever",
        cookies,
        [],
        dag_id="whatever",
        scene_id_keys=["scene_id", "split_id"],
        reduce_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )

    assert watcher.reduced_scene_id_keys == ["scene_id"]


@pytest.mark.asyncio
async def test_reduced_scene_id_keys_2(cookies):
    watcher = ReducibleRestAPIWatcher(
        "http://127.0.0.1:8080",
        "whatever",
        cookies,
        [],
        dag_id="whatever",
        scene_id_keys=["scene_id", "split_id", "object_id"],
        reduce_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )

    assert watcher.reduced_scene_id_keys == ["scene_id", "object_id"]


@pytest.mark.asyncio
async def test_reduce(cookies):
    watcher = ReducibleRestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [],
        dag_id="dag_expandable",
        scene_id_keys=["scene_id", "split_id"],
        reduce_by=XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    )

    upstream_scenes = [{"scene_id": "20231220_1101", "split_id": split_id} for split_id in range(3)]
    reduced_scenes = await watcher.reduce(upstream_scenes)
    assert reduced_scenes == [{"scene_id": "20231220_1101"}]

