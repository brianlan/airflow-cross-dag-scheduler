from typing import List

import pandas as pd

from .xcom_query import XComQuery


class Expandable:
    def __init__(self, expand_by: XComQuery, *args, **kwargs) -> None:
        """The class is a Mixin class, and can not be used alone.

        Parameters
        ----------
        expand_by : str
            description to how to get the xcom_values to expand the original fetched data by UpstreamSensor.
            For example:
                expand_by = {
                    "dag_id": "dag_split_map_generator",
                    "task_id": "generate_split_map",
                    "xcom_key": "return_value",
                    "refer_name": "split_id"
                }
        """
        super().__init__(*args, **kwargs)
        self.expand_by = expand_by
        assert 'base_scene_id_keys' in kwargs, "base_scene_id_keys should be provided for Expandable"

    async def sense(self, state: str = None) -> pd.DataFrame:
        raw_df = await super().sense(state=state)
        expanded_df = await self.expand(raw_df)
        return expanded_df

    async def expand(self, df: pd.DataFrame) -> List[dict]:
        """The expansion will based on the same batch_id and scene_id_keys, expand the dag_run by the xcom values"""
        xcom_expanded_df = await self.expand_by.query(self.api_url, self.batch_id, self.cookies, state="success")
        
        if len(xcom_expanded_df) == 0:
            return pd.DataFrame([])
        
        merged_df = pd.merge(df, xcom_expanded_df, how="inner", on=self.base_scene_id_keys).reset_index(drop=True)
        return merged_df
