from typing import List

import pandas as pd

from .base import UpstreamSensor


class StaticSceneListSensor(UpstreamSensor):
    def __init__(self, api_url: str, batch_id: str, cookies: dict, scene_list: List[dict] = None) -> None:
        """_summary_

        Parameters
        ----------
        batch_id : str
            the batch_id that is interested about, for example "baidu_integration_test"
        scene_list : List[dict]
            predefined scene list
        """
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.scene_list = scene_list

    async def sense(self, state: str = None) -> pd.DataFrame:
        scene_list_df = pd.DataFrame.from_records(self.scene_list)
        scene_list_df.loc[:, "batch_id"] = self.batch_id
        scene_list_df.loc[:, "state"] = "success"
        if state:
            scene_list_df = scene_list_df[scene_list_df.state == state].reset_index(drop=True)
        return scene_list_df

    @property
    def query_key_values(self) -> list[str]:
        return {"batch_id": self.batch_id}
