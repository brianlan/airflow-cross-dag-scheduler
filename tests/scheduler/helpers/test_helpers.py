import pandas as pd

from scheduler.helpers.base import async_read_cookie_session, is_in_df, extract_values

async def test_async_read_cookie_session():
    result = await async_read_cookie_session("conf/cookie_session")
    assert result == "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"


def test_is_in_df():
    df = pd.concat(
            [
                pd.DataFrame(
                    {
                        "dag_id": ["dag_for_unittest"] * 2,
                        "dag_run_id": ["manual__2023-12-20T03:38:06+00:00"]  + ["fixed_a001"] ,
                        "dag_run_state": ["success"] * 2,
                        "scene_id": ["20231220_1101"]  + ["underground_1220"] ,
                        "task_id": [ "fisheye.task_inside_2"] * 2,
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
                        "task_id": ["haha"],
                        "task_instance_state": ["failed"],
                        "state": ["success"],
                    }
                ),
            ]
        ).reset_index(drop=True)
    assert is_in_df({"task_instance_state": "success", "scene_id": "20231220_1101"}, df)
    assert not is_in_df({"task_instance_state": "failed", "scene_id": "20231220_1101", "dag_id": "dag_for_unittest"}, df)


def test_extract_values():
    assert extract_values('[{"split_map": 0},{"split_map": 1}, {"split_map": 2}]') == [0, 1, 2]
    assert extract_values('[0,2,3,1]') == [0, 2, 3, 1]
    assert extract_values('[{"split_map": 0, "obj_id": 100},{"split_map": 1, "obj_id": 200}]') == [0, 1]
    assert extract_values("[{'split_map': 0, 'obj_id': 100},{'split_map': 1, 'obj_id': 200}]") == [0, 1]

# def test_read_cookie_session():
#     assert read_cookie_session() == "b9c867dc-5319-4ad4-97e0-6474260b10de.x5LW6WQ0sSpk_vARkCsQzQfpXDE"
