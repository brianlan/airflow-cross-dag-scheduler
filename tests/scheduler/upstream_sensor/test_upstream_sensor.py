import pytest
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

from scheduler.upstream_sensor.dag_sensor import DagSensor
from scheduler.upstream_sensor.task_sensor import TaskSensor
from scheduler.upstream_sensor.xcom_query import XComQuery
from scheduler.upstream_sensor.static_scene_list_sensor import StaticSceneListSensor
from scheduler.helpers.aiohttp_requests import Non200Response


@pytest.fixture
def cookies():
    return {"session": "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"}


@pytest.mark.asyncio
async def test_dag_sensor(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest")
    status_df = await sensor.sense()
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
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
async def test_dag_sensor_with_state_success(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", "baidu_integration_test", cookies, dag_id="downstream")
    status_df = await sensor.sense(state="success")
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["batch_id", "dag_id", "dag_run_id", "dag_run_state", "scene_id", "state"]],
        pd.DataFrame(
            {
                "batch_id": ["baidu_integration_test"],
                "dag_id": ["downstream"],
                "dag_run_id": ["fixed_d001"],
                "dag_run_state": ["success"],
                "scene_id": ["20231220_1101"],
                "state": ["success"],
            }
        ),
    )


@pytest.mark.asyncio
async def test_dag_sensor_when_dag_not_exist(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_id_does_not_exist")
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame([]),
    )


@pytest.mark.asyncio
async def test_dag_sensor_when_batch_id_not_match(cookies):
    sensor = DagSensor("http://127.0.0.1:8080", "batch_id_not_exist", cookies, dag_id="downstream")
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame([]),
    )


@pytest.mark.asyncio
async def test_task_sensor(cookies):
    sensor = TaskSensor(
        "http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"
    )
    status_df = await sensor.sense()
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.DataFrame(
            {
                "dag_id": ["dag_for_unittest"] * 2,
                "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"] + ["fixed_a001"],
                "dag_run_state": ["success"] * 2,
                "scene_id": ["20231220_1101"] + ["underground_1220"],
                "task_id": ["fisheye.task_inside_2"] * 2,
                "task_instance_state": ["success"] * 2,
                "state": ["success"] * 2,
            }
        ),
    )


@pytest.mark.asyncio
async def test_task_sensor_with_state_failed(cookies):
    sensor = TaskSensor("http://127.0.0.1:8080", "not_tracked", cookies, dag_id="downstream", task_id="random_fail")
    status_df = await sensor.sense(state="failed")
    status_df.loc[:, "scene_id"] = status_df.conf.map(lambda x: x.get("scene_id"))
    pd.testing.assert_frame_equal(
        status_df[
            ["batch_id", "dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]
        ],
        pd.DataFrame(
            {
                "batch_id": ["not_tracked"],
                "dag_id": ["downstream"],
                "dag_run_id": ["scene_id:20231220_7777"],
                "dag_run_state": ["failed"],
                "scene_id": ["20231220_7777"],
                "task_id": ["random_fail"],
                "task_instance_state": ["failed"],
                "state": ["failed"],
            }
        ),
    )


@pytest.mark.asyncio
async def test_task_sensor_when_no_dag_run(cookies):
    sensor = TaskSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_id_does_not_exist", task_id="random_fail")
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame([]),
    )


@pytest.mark.asyncio
async def test_task_sensor_when_batch_id_not_match(cookies):
    sensor = TaskSensor(
        "http://127.0.0.1:8080", "batch_id_not_exist", cookies, dag_id="downstream", task_id="random_fail"
    )
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame([]),
    )


@pytest.mark.asyncio
async def test_task_sensor_when_no_task(cookies):
    sensor = TaskSensor(
        "http://127.0.0.1:8080", "not_tracked", cookies, dag_id="downstream", task_id="task_id_not_exist"
    )
    with pytest.raises(Non200Response):
        _ = await sensor.sense(state="success")


@pytest.mark.asyncio
async def test_static_scene_list_sensor():
    sensor = StaticSceneListSensor(
        "http", "a_batch_id", {}, scene_list=[{"scene_id": "scn_001"}, {"scene_id": "scn_002"}]
    )
    status_df = await sensor.sense(state="success")
    pd.testing.assert_frame_equal(
        status_df,
        pd.DataFrame(
            {
                "scene_id": ["scn_001", "scn_002"],
                "batch_id": ["a_batch_id"] * 2,
                "state": ["success"] * 2,
            }
        ),
    )


@pytest.mark.asyncio
async def test_get_mixed_sensor_results(cookies):
    upstream_sensors = [
        TaskSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest", task_id="fisheye.task_inside_2"),
        DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_for_unittest_another"),
    ]
    status_df_list = [await sensor.sense(state="success") for sensor in upstream_sensors]
    status_df = pd.concat(status_df_list).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        status_df[["dag_id", "dag_run_id", "dag_run_state", "scene_id", "task_id", "task_instance_state", "state"]],
        pd.concat(
            [
                pd.DataFrame(
                    {
                        "dag_id": ["dag_for_unittest"] * 2,
                        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"] + ["fixed_a001"],
                        "dag_run_state": ["success"] * 2,
                        "scene_id": ["20231220_1101"] + ["underground_1220"],
                        "task_id": ["fisheye.task_inside_2"] * 2,
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
async def test_get_all_success_upstream_when_upstream_empty(cookies):
    upstream_sensors = [
        TaskSensor(
            "http://127.0.0.1:8080",
            "batch_id_not_exist",
            cookies,
            dag_id="dag_for_unittest",
            task_id="fisheye.task_inside_2",
        ),
        DagSensor("http://127.0.0.1:8080", None, cookies, dag_id="dag_id_not_exist"),
    ]
    status_df_list = [await sensor.sense(state="success") for sensor in upstream_sensors]
    status_df = pd.concat(status_df_list).reset_index(drop=True)

    assert len(status_df) == 0


@pytest.mark.asyncio
async def test_xcom_query(cookies):
    xquery = XComQuery("dag_split_map_generator", "generate_split_map", "return_value", "split_id")
    df = await xquery.query("http://127.0.0.1:8080", "baidu_integration_test", cookies)
    gt = pd.DataFrame({
        # "batch_id": ["baidu_integration_test"] * 5,
        # "dag_id": ["dag_split_map_generator"] * 5,
        "scene_id": ["20231220_1101"] * 5,
        # "dag_run_id": ["manual__2023-12-25T10:27:12+00:00"] * 5,
        "split_id": [0, 1, 2, 3, 4],
    })
    gt.loc[:, "split_id"] = gt.split_id.astype("object")
    pd.testing.assert_frame_equal( df, gt )


@pytest.mark.asyncio
async def test_xcom_query_task_instance_not_exist(cookies):
    xquery = XComQuery("dag_split_map_generator", "task_id_not_exist", "return_value", "split_id")
    df = await xquery.query("http://127.0.0.1:8080", "baidu_integration_test", cookies)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
