from typing import List

import pandas as pd

from .xcom_query import XComQuery


class Reducible:
    def __init__(self, reduce_by: XComQuery, *args, **kwargs) -> None:
        """The class is a Mixin class, and can not be used alone.

        Parameters
        ----------
        reduce_by : str
            description to how to get the xcom_values to expand the original fetched data by UpstreamSensor.
            For example:
                reduce_by = {
                    "dag_id": "dag_split_map_generator",
                    "task_id": "generate_split_map",
                    "xcom_key": "return_value",
                    "refer_name": "split_id"
                }
        """
        super().__init__(*args, **kwargs)
        self.reduce_by = reduce_by
        assert 'base_scene_id_keys' in kwargs, "base_scene_id_keys should be provided for Expandable"

    async def sense(self, state: str = None) -> pd.DataFrame:
        raw_df = await super().sense(state=state)
        expanded_df = await self.reduce(raw_df)
        return expanded_df

    async def reduce(self, df: pd.DataFrame) -> List[dict]:
        """The reduction will based on the same batch_id and scene_id_keys"""
        xcom_expanded_df = await self.reduce_by.query(self.api_url, self.batch_id, self.cookies, state="success")
        
        if len(xcom_expanded_df) == 0:
            return pd.DataFrame([])
        
        expanded_scene_id_keys = self.base_scene_id_keys + [self.reduce_by.refer_name]
        _scene_id_keys = self.base_scene_id_keys[0] if len(self.base_scene_id_keys) == 1 else self.base_scene_id_keys # to prevent pandas warning
        merged_df = pd.merge(df, xcom_expanded_df, how="outer", on=expanded_scene_id_keys)
        # merged_df.loc[:, self.reduce_by.refer_name] = xcom_expanded_df[self.reduce_by.refer_name]

        # critical_columns = ["batch_id", "dag_id", "task_id"]
        # critical_columns = set(critical_columns) & set(df.columns)
        other_columns = [c for c in merged_df.columns if c not in expanded_scene_id_keys and c != 'state']
        reduced_df = merged_df.groupby(_scene_id_keys).agg({
            **{col: list for col in other_columns},
            'state': lambda x: 'success' if all(s == 'success' for s in x) else 'failed'
        }).reset_index()
        
        return reduced_df
