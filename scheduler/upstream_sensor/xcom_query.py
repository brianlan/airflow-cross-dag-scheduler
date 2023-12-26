from typing import List
from dataclasses import dataclass

import pandas as pd

from ..helpers.airflow_api import get_dag_runs, get_xcom
from ..helpers.base import extract_values


@dataclass
class XComQuery:
    dag_id: str
    task_id: str
    xcom_key: str
    out_col_name: str

    async def query(self, api_url: str, batch_id: str, cookies: dict, state: str = None) -> pd.DataFrame:
        expand_dag_run_df = await get_dag_runs(
            api_url, batch_id, self.dag_id, cookies, to_dataframe=True, flatten_conf=True
        )

        if len(expand_dag_run_df) == 0:
            return pd.DataFrame([])

        xcom_values_list = []
        for idx, row in expand_dag_run_df.iterrows():
            xcom = await get_xcom(
                api_url,
                row.dag_id,
                row.dag_run_id,
                self.task_id,
                cookies,
                xcom_key=self.xcom_key,
                to_dataframe=False,
            )
            assert len(xcom) == 1
            xcom_values = extract_values(xcom[0]["value"])
            xcom_values_list.append(xcom_values)

        assert len(xcom_values_list) == len(expand_dag_run_df)
        expand_dag_run_df.loc[:, self.out_col_name] = xcom_values_list
        expand_dag_run_df = expand_dag_run_df.explode(self.out_col_name, ignore_index=True)
        expand_dag_run_df.drop(expand_dag_run_df[expand_dag_run_df[self.out_col_name].isnull()].index, inplace=True)

        if state is not None:
            expand_dag_run_df = expand_dag_run_df[expand_dag_run_df.dag_run_state == state].reset_index(drop=True)

        output_columns = ["batch_id", "scene_id", self.out_col_name]

        return expand_dag_run_df[output_columns]
