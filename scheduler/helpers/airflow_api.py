from typing import Union, List, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from . import aiohttp_requests as ar

pd.set_option("display.max_columns", None)


async def get_dag_runs(
    api_url: str,
    batch_id: str,
    dag_id: str,
    cookies: dict,
    to_dataframe: bool = False,
    flatten_conf: bool = False,
) -> Union[List[dict], pd.DataFrame]:
    """Get all the DagRuns of `dag_id` with the same batch_id as batch_id using Airflow RESTAPI:
    http://{api_url}/api/v1/dags/{dag_id}/dagRuns

    Parameters
    ----------
    api_url : str
        api endpoint url
    batch_id : str
        the batch_id that the watcher is caring about, for example "baidu_integration_test"
    dag_id : str
        dag id
    cookies: dict
        cookies for authentication
    to_dataframe : bool, optional
        if True, will convert list of dagruns (dict) into pandas.DataFrame, by default False.

    Returns
    -------
    Union[List[dict], pd.DataFrame]
        dag runs info
    """
    assert to_dataframe or not flatten_conf, "flatten_conf only works when to_dataframe is True"

    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns"
    status, json_data = await ar.get(url, cookies=cookies)
    dag_runs = json_data["dag_runs"]
    dag_runs = [dr for dr in dag_runs if dr["conf"].get("batch_id") == batch_id]
    if to_dataframe:
        if len(dag_runs) == 0:
            dag_runs = pd.DataFrame([])
        else:
            dag_runs = pd.DataFrame.from_records(dag_runs)
            dag_runs.loc[:, "dag_run_state"] = dag_runs.state
            if flatten_conf:
                dag_runs = pd.concat([dag_runs, dag_runs["conf"].apply(pd.Series)], axis=1)
    return dag_runs


# async def get_task_instances(
#     api_url: str, dag_id: str, dag_run_id: str, cookies: dict, to_dataframe: bool = False
# ) -> Union[List[dict], pd.DataFrame]:
#     """Get single task instance status of a DagRun using Airflow RestAPI:
#     http://{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances

#     Parameters
#     ----------
#     api_url : str
#         api endpoint url
#     dag_id : str
#         dag id
#     dag_run_id : str
#         dagRun id
#     cookies: dict
#         cookies for authentication
#     to_dataframe : bool, optional
#         if True, will convert list of dagruns (dict) into pandas.DataFrame, by default False.

#     Returns
#     -------
#     Union[List[dict], pd.DataFrame]
#         list of task instance info
#     """
#     url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
#     status, json_data = await ar.get(url, cookies=cookies)
#     task_instances = json_data["task_instances"]
#     if to_dataframe:
#         task_instances = pd.DataFrame.from_records(task_instances)
#         task_instances.rename(columns={"state": "task_instance_state"}, inplace=True)
#     return task_instances


async def get_task_instance(
    api_url: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    cookies: dict,
    to_dataframe: bool = False,
    columns_to_drop: Sequence[str] = ('executor_config', 'rendered_fields'),
) -> Union[List[dict], pd.DataFrame]:
    """Get specific task instance status of a DagRun using Airflow RestAPI:
    http://{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}

    Parameters
    ----------
    api_url : str
        api endpoint url
    dag_id : str
        dag id
    dag_run_id : str
        dagRun id
    task_id : str
        task id
    cookies: dict
        cookies for authentication
    to_dataframe : bool, optional
        if True, will convert list of dagruns (dict) into pandas.DataFrame, by default False.
    columns_to_drop: 
        columns to drop, due to the reduce operation, some unhashable columns must be dropped
    Returns
    -------
    Union[List[dict], pd.DataFrame]
        list of task instance info
    """
    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
    status, ti = await ar.get(url, cookies=cookies)
    ti = [ti]
    if to_dataframe:
        ti = pd.DataFrame.from_records(ti)
        ti.loc[:, "task_instance_state"] = ti.state
    if columns_to_drop:
        ti.drop(columns=list(columns_to_drop), inplace=True)
    return ti


async def trigger_dag(api_url: str, dag_id: str, cookies: dict, dag_conf: dict = None, dag_run_id: str = None) -> None:
    """Trigger a DagRun using Airflow RestAPI:
    https://{api_url}/api/v1/dags/{dag_id}/dagRuns
    If the dag is paused, nothing will happen.

    Parameters
    ----------
    api_url : str
        api endpoint url
    dag_id : str
        dag id
    cookies: dict
        cookies for authentication
    dag_conf : dict, optional
        conf dict that passed to DagRun when triggering, by default None
    dag_run_id : str, optional
        if specified, will use this as DagRunId, by default None
    """
    dag_info = await get_dag_info(api_url, dag_id, cookies)
    if dag_info["is_paused"]:
        msg = f"DAG {dag_id} is paused, skip triggering."
        logger.info(msg)
        return 200, {"message": msg}

    dag_conf = dag_conf or {}

    dtype_map = {np.int64: int, np.int32: int, np.float64: float, np.float32: float, str: str}

    dag_conf = {
        k: dtype_map[type(dag_conf[k])](dag_conf[k]) for k in dag_conf
    }  # convert dtype to prevent Airflow complaining about json serialization

    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": dag_conf}
    if dag_run_id:
        payload["dag_run_id"] = dag_run_id

    return await ar.post(url, payload, cookies=cookies)


async def get_dag_info(api_url: str, dag_id: str, cookies: dict) -> None:
    """Get the basic info of a dag using Airflow RestAPI:
    https://{api_url}/api/v1/dags/{dag_id}
    If the dag is paused, nothing will happen.

    Parameters
    ----------
    api_url : str
        api endpoint url
    dag_id : str
        dag id
    cookies: dict
        cookies for authentication
    """
    url = f"{api_url}/api/v1/dags/{dag_id}"
    status, json_data = await ar.get(url, cookies=cookies)
    return json_data


async def get_xcom(
    api_url: str,
    dag_id: str,
    dag_run_id: str,
    xcom_task_id: str,
    cookies: dict,
    to_dataframe: bool = False,
    xcom_key: str = "return_value",
) -> Union[List[dict], pd.DataFrame]:
    """Get xcom value of a specific xcom_key using Airflow RestAPI:
    http://{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}

    Parameters
    ----------
    api_url : str
        api endpoint url
    dag_id : str
        dag id
    dag_run_id : str
        dagRun id
    xcom_task_id : str
        task id that produce (push) the xcom
    xcom_key: str
        the xcom key
    cookies: dict
        cookies for authentication
    to_dataframe : bool, optional
        if True, will convert list of dagruns (dict) into pandas.DataFrame, by default False.

    Returns
    -------
    Union[List[dict], pd.DataFrame]
        xcom value dict of dataframe
    """
    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{xcom_task_id}/xcomEntries/{xcom_key}"
    status, data = await ar.get(url, cookies=cookies)
    data = [data]
    if to_dataframe:
        data = pd.DataFrame.from_records(data)
    return data
