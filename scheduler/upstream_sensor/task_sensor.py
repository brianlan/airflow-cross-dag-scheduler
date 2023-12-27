from typing import List

import pandas as pd

from scheduler.upstream_sensor.xcom_query import XComQuery
from ..helpers.airflow_api import get_dag_runs, get_task_instance
from .base import UpstreamSensor
from .expandable import Expandable
from .reducible import Reducible


class TaskSensor(UpstreamSensor):
    def __init__(
        self, api_url: str, batch_id: str, cookies: dict, dag_id: str = None, task_id: str = None, base_scene_id_keys: List[str] = None, **kwargs
    ) -> None:
        """Used to sense the upstream DagRun's TaskInstance state

        Parameters
        ----------
        api_url : str
            api endpoint url
        batch_id : str
            the batch_id that the watcher is caring about, for example "baidu_integration_test"
        dag_id : str
            the upstream dag_id that the sensor is going to sense its state
        task_id : str
            the task_id of upstream dag_id that the sensor is going to sense its state
        cookies : dict
            cookie dict that required by Airflow REST API for authentication
        base_scene_id_keys : List[str]
            The keys that determines a scene.
        """
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.base_scene_id_keys = base_scene_id_keys
        self.dag_id = dag_id
        self.task_id = task_id
        self.cookies = cookies

    async def sense(self, state: str = None) -> pd.DataFrame:
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True, flatten_conf=True)

        if len(dag_run_df) == 0:
            return pd.DataFrame([])

        task_instances = []
        for dag_run_id in dag_run_df["dag_run_id"]:
            task_instances.append(
                await get_task_instance(
                    self.api_url, self.dag_id, dag_run_id, self.task_id, self.cookies, to_dataframe=True
                )
            )
        task_instance_df = pd.concat(task_instances).reset_index(drop=True)

        assert len(dag_run_df) == len(task_instance_df), "#taskInstances should match #dagRuns"

        status_df = pd.merge(dag_run_df, task_instance_df, how="inner", on=["dag_id", "dag_run_id"])
        if state is not None:
            status_df = status_df[status_df.task_instance_state == state].reset_index(drop=True)
        status_df.loc[:, "state"] = status_df.task_instance_state
        # status_df.loc[:, "state"] = status_df.apply(
        #     lambda x: x.dag_run_state if pd.isnull(x.task_instance_state) else x.task_instance_state, axis=1
        # )

        return status_df

    @property
    def query_key_values(self) -> list[str]:
        return {"batch_id": self.batch_id, "dag_id": self.dag_id, "task_id": self.task_id}


class ExpandableTaskSensor(Expandable, TaskSensor):
    def __init__(self, expand_by: XComQuery, *args, **kwargs) -> None:
        super().__init__(expand_by, *args, **kwargs)


class ReducibleTaskSensor(Reducible, TaskSensor):
    def __init__(self, reduce_by: XComQuery, *args, **kwargs) -> None:
        super().__init__(reduce_by, *args, **kwargs)
