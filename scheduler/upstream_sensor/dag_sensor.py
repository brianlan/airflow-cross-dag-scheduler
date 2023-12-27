from typing import List

import pandas as pd
from pandas.core.api import DataFrame as DataFrame

from ..helpers.airflow_api import get_dag_runs
from .base import UpstreamSensor
from .expandable import Expandable
from .reducible import Reducible


class DagSensor(UpstreamSensor):
    def __init__(
        self,
        api_url: str,
        batch_id: str,
        cookies: dict,
        *,
        dag_id: str = None,
        base_scene_id_keys: List[str] = None,
        **kwargs
    ) -> None:
        """Used to sense the upstream DagRun's state

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
        base_scene_id_keys : List[str], optional
            The keys that determines a scene.
        """
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.base_scene_id_keys = base_scene_id_keys
        self.dag_id = dag_id
        self.cookies = cookies

    async def sense(self, state: str = None) -> pd.DataFrame:
        dag_run_df = await get_dag_runs(
            self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True, flatten_conf=True
        )

        if len(dag_run_df) == 0:
            return pd.DataFrame([])

        if state is not None:
            dag_run_df = dag_run_df[dag_run_df.dag_run_state == state].reset_index(drop=True)

        return dag_run_df

    @property
    def query_key_values(self) -> list[str]:
        return {"batch_id": self.batch_id, "dag_id": self.dag_id}


class ExpandableDagSensor(Expandable, DagSensor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class ReducibleDagSensor(Reducible, DagSensor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
