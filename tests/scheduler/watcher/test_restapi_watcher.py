import pytest
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

from scheduler.watcher.restapi_watcher import RestAPIWatcher
from scheduler.upstream_sensor.dag_sensor import DagSensor, ExpandableDagSensor
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


@pytest.mark.asyncio
async def test_get_existing_scenes_multi_scene_id_keys(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [],
        dag_id="dag_expandable",
        scene_id_keys=["scene_id", "split_id"],
    )
    existing_scenes = await watcher.get_existing_scenes()
    assert existing_scenes == [
        {"scene_id": "20231220_1101", "split_id": 1, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 0, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 2, "state": "failed"},
        {"scene_id": "20231220_1101", "split_id": 3, "state": "success"},
        {"scene_id": "20231220_1101", "split_id": 4, "state": "success"},
    ]


@pytest.mark.asyncio
async def test_get_all_ready_scene_with_expandable_upstream_sensor(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            ExpandableDagSensor(
                "http://127.0.0.1:8080", 
                "baidu_integration_test", 
                cookies, 
                dag_id="dag_for_unittest_another",
                base_scene_id_keys=["scene_id"],
                expand_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
            )
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id", "split_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert result == [
        {"scene_id": "20231220_1101", "split_id": 0},
        {"scene_id": "20231220_1101", "split_id": 1},
        {"scene_id": "20231220_1101", "split_id": 2},
        {"scene_id": "20231220_1101", "split_id": 3},
        {"scene_id": "20231220_1101", "split_id": 4},
    ]


@pytest.mark.asyncio
async def test_get_all_ready_scene_with_expandable_upstream_sensor_2(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            ExpandableDagSensor(
                "http://127.0.0.1:8080", 
                "baidu_integration_test", 
                cookies, 
                dag_id="dag_for_unittest_another",
                base_scene_id_keys=["scene_id"],
                expand_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
            ),
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_expandable", task_id="world"), 
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id", "split_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert result == [
        {"scene_id": "20231220_1101", "split_id": 0},
        {"scene_id": "20231220_1101", "split_id": 1},
        {"scene_id": "20231220_1101", "split_id": 3},
        {"scene_id": "20231220_1101", "split_id": 4},
    ]


@pytest.mark.asyncio
async def test_get_all_ready_scene_with_expandable_upstream_sensor_3(cookies):
    watcher = RestAPIWatcher(
        "http://127.0.0.1:8080",
        "baidu_integration_test",
        cookies,
        [
            ExpandableDagSensor(
                "http://127.0.0.1:8080", 
                "baidu_integration_test", 
                cookies, 
                dag_id="dag_for_unittest_another",
                base_scene_id_keys=["scene_id"],
                expand_by={"dag_id": "dag_split_map_generator", "task_id": "generate_split_map", "xcom_key": "return_value", "refer_name": "split_id"},
            ),
            TaskSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="dag_expandable", task_id="hello"), 
        ],
        dag_id="downstream",
        scene_id_keys=["scene_id", "split_id"],
    )
    result = await watcher.get_all_upstream_ready_scenes()
    assert result == [
        {"scene_id": "20231220_1101", "split_id": 0},
        {"scene_id": "20231220_1101", "split_id": 1},
        {"scene_id": "20231220_1101", "split_id": 2},
        {"scene_id": "20231220_1101", "split_id": 3},
        {"scene_id": "20231220_1101", "split_id": 4},
    ]


@pytest.mark.asyncio
async def test_scene_id_dtypes():
    watcher = RestAPIWatcher( "a", "a", None, [], dag_id="a", scene_id_keys=["scene_id"], scene_id_dtypes=["str"], )
    assert watcher.convert_dtypes({"scene_id": 123.0}) == {"scene_id": "123.0"}
    watcher = RestAPIWatcher( "a", "a", None, [], dag_id="a", scene_id_keys=["scene_id"], scene_id_dtypes=["int"], )
    assert watcher.convert_dtypes({"scene_id": 123.0}) == {"scene_id": 123}
    watcher = RestAPIWatcher( "a", "a", None, [], dag_id="a", scene_id_keys=["scene_id"], scene_id_dtypes=["float"], )
    assert watcher.convert_dtypes({"scene_id": "123.0"}) == {"scene_id": 123.0}
    watcher = RestAPIWatcher( "a", "a", None, [], dag_id="a", scene_id_keys=["scene_id", "split_id"], scene_id_dtypes=["str", "int"], )
    assert watcher.convert_dtypes({"scene_id": 123.0, "split_id": 4.0}) == {"scene_id": "123.0", "split_id": 4}
