from typing import List

import pandas as pd

from ..helpers.airflow_api import get_dag_runs, get_xcom
from .base import UpstreamSensor


class XcomSensor(UpstreamSensor):
    def __init__(
        self, api_url: str, batch_id: str, cookies: dict, dag_id: str = None, task_id: str = None, expand_id_keys: List[str] = None, **kwargs
    ) -> None:
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.dag_id = dag_id
        self.task_id = task_id
        self.cookies = cookies
        self.expand_id_keys = expand_id_keys

    async def sense(self, state: str = None) -> pd.DataFrame:
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True, flatten_conf=True)

        if len(dag_run_df) == 0:
            return pd.DataFrame([])

        xcom_values = []
        for dag_run_id in dag_run_df["dag_run_id"]:
            xcom = await get_xcom(self.api_url, self.dag_id, dag_run_id, self.task_id, self.cookies, xcom_key=k, to_dataframe=False)
            xcom_values.append(xcom["value"])
        xcom_values_df = pd.concat(xcom_values).reset_index(drop=True)

        assert len(dag_run_df) == len(xcom_values_df), "#XcomValues should match #dagRuns"
        merged = pd.merge(dag_run_df, xcom_values_df, how="inner", on=["dag_id", "dag_run_id"])
        return merged

    @property
    def query_key_values(self) -> list[str]:
        return {"batch_id": self.batch_id, "dag_id": self.dag_id, "task_id": self.task_id                                                                                                               }
