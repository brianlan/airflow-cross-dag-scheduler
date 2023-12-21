from typing import Sequence, Union, List

import pandas as pd

from . import aiohttp_requests as ar


async def get_dag_runs(
    api_url: str,
    batch_id: str,
    dag_id: str,
    cookies: dict,
    to_dataframe: bool = False,
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
    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns"
    status, json_data = await ar.get(url, cookies=cookies)
    dag_runs = json_data["dag_runs"]
    dag_runs = [dr for dr in dag_runs if dr["conf"].get("batch_id") == batch_id]
    if to_dataframe:
        for dr in dag_runs:
            dr["batch_id"] = dr["conf"].get("batch_id")
        dag_runs = pd.DataFrame.from_records(dag_runs)
        dag_runs.rename(columns={"state": "dag_run_state"}, inplace=True)
    return dag_runs


async def get_task_instances(
    api_url: str, dag_id: str, dag_run_id: str, cookies: dict, to_dataframe: bool = False
) -> Union[List[dict], pd.DataFrame]:
    """Get single task instance status of a DagRun using Airflow RestAPI:
    http://{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances

    Parameters
    ----------
    api_url : str
        api endpoint url
    dag_id : str
        dag id
    dag_run_id : str
        dagRun id
    cookies: dict
        cookies for authentication
    to_dataframe : bool, optional
        if True, will convert list of dagruns (dict) into pandas.DataFrame, by default False.

    Returns
    -------
    Union[List[dict], pd.DataFrame]
        list of task instance info
    """
    url = f"{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    status, json_data = await ar.get(url, cookies=cookies)
    task_instances = json_data["task_instances"]
    if to_dataframe:
        task_instances = pd.DataFrame.from_records(task_instances)
        task_instances.rename(columns={"state": "task_instance_state"}, inplace=True)
    return task_instances


async def get_task_instance(
    api_url: str, dag_id: str, dag_run_id: str, task_id: str, cookies: dict, to_dataframe: bool = False
) -> Union[List[dict], pd.DataFrame]:
    """Get all the task instance status of a DagRun using Airflow RestAPI:
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
        ti.rename(columns={"state": "task_instance_state"}, inplace=True)
    return ti
