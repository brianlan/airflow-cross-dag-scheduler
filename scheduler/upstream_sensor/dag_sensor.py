from typing import Any

import pandas as pd

from ..helpers.airflow_api import get_dag_runs
from .base import UpstreamSensor


class DagSensor(UpstreamSensor):
    def __init__(self, api_url: str, batch_id: str, dag_id: str, cookies: dict, scene_id_keys) -> None:
        """_summary_

        Parameters
        ----------
        api_url : str
            api endpoint url
        batch_id : str
            the batch_id that the watcher is caring about, for example "baidu_integration_test"
        dag_id : str
            the upstream dag_id that the sensor is going to sense its state
        cookies : dict
            cookie dict that required by Airflow REST API for authentication
        """
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.dag_id = dag_id
        self.cookies = cookies
        self.scene_id_keys = scene_id_keys

    async def sense(self) -> pd.DataFrame:
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True, scene_id_keys=self.scene_id_keys)
        dag_run_df.loc[:, "state"] = dag_run_df.dag_run_state
        return dag_run_df