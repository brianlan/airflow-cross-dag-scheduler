from typing import List
import time

import pandas as pd
from loguru import logger

from ..helpers.base import is_in_df
from ..helpers.airflow_api import get_dag_runs, trigger_dag
from .restapi_watcher import RestAPIWatcher
from ..upstream_sensor.xcom_query import XComQuery
from ..upstream_sensor.base import UpstreamSensor
from .base import WatchResult


class ExpandableRestAPIWatcher(RestAPIWatcher):
    def __init__(
        self,
        *args,
        expand_by: XComQuery = None,
        **kwargs,
    ) -> None:
        """__init__ of ExpandableRestAPIWatcher

        Parameters
        ----------
        api_url : str
            api endpoint url
        batch_id : str
            the batch_id that the watcher is caring about, for example "baidu_integration_test"
        dag_id : str
            the dag_id that the watcher is watching
        upstream_sensors : List[UpstreamSensor]
            descriptions of the upstream dag / task
            A demonstration example (we don't factually use that):
            upstream = [
                {"dag_id": "generate_base_data", "task_id": "generate_image"},
                {"dag_id": "optimize_ipm"},
                {"dag_id": "fisheye_segmentation", "task_group_id": "tg_nvidia_segmentation"}
            ]
            For each item in `upstream`, if task_id provided, this upstream is considered as success only when the task is success.
            If task_id is not provided, this upstream is considered as success when the dag is success.
        scene_id_keys : List[str]
            The keys that determines a scene.
        expand_by: XComQuery
            description to how to get the xcom_values to expand the dag_run.
            For example:
                expand_by = {
                    "dag_id": "dag_split_map_generator",
                    "task_id": "generate_split_map",
                    "xcom_key": "return_value",
                    "out_col_name": "split_id"
                }
        cookie_session_path : str
            the path to the cookie_session file that required by Airflow REST API for authentication
        max_running_dag_runs : int, optional
            the maximum number of running dag_runs that the watcher will keep, by default 3
        triggered_dag_run_id_style : str, optional
            Valid choices: ["timestamp", "scene_id_keys", "scene_id_keys_with_time"], by default "scene_id_keys_with_time"
        watch_interval : int
            time interval (in seconds) between each watch, by default 10
        """
        super().__init__(*args, **kwargs)
        self.expand_by = expand_by

    def __repr__(self) -> str:
        return f"ExpandableRestAPIWatcher({self.dag_id}, expand_by={self.expand_by})"

    @property
    def expanded_scene_id_keys(self) -> List[str]:
        expanded_scene_id_keys = self.scene_id_keys
        if self.expand_by:
            expanded_scene_id_keys.append(self.expand_by.out_col_name)
        return expanded_scene_id_keys

    async def expand(self, df: pd.DataFrame) -> pd.DataFrame:
        """The expansion will based on the same batch_id and scene_id_keys, expand the dag_run by the xcom values"""
        expand_dag_run_df = await self.expand_by.query(self.api_url, self.batch_id, self.cookies, state="success")
        
        if len(expand_dag_run_df) == 0:
            return pd.DataFrame([])
        
        merged = pd.merge(df, expand_dag_run_df, how="inner", on=["batch_id"] + self.scene_id_keys).reset_index(drop=True)
        return merged

    async def watch(self) -> WatchResult:
        logger.info(f"[Watcher {self.dag_id}] Start watching..")
        ready_scenes = await self.get_all_upstream_ready_scenes()
        ready_scenes = await self.expand(ready_scenes)
        existing_scenes = await self.get_existing_scenes()
        running_scenes = [s for s in existing_scenes if s["state"] == "running"]
        trigger_quota = self.max_running_dag_runs - len(running_scenes)
        result = WatchResult()
        if len(ready_scenes) == 0 or trigger_quota <= 0:
            result.action = "watch"
            return result

        # trigger the first scene that meets the requirement
        for ready_scene in ready_scenes:
            has_been_triggered = [e for e in existing_scenes if all(e[k] == ready_scene[k] for k in self.expanded_scene_id_keys)]
            if len(has_been_triggered) == 0 and trigger_quota > 0:
                result.action = "trigger"
                result.context = ready_scene
                return result

        return result

    async def get_existing_scenes(self) -> List[dict]:
        """Get all the existing scenes of self.dag_id

        Returns
        -------
        List[dict]
            list of existing scenes, each scene is a dict of {scene_id_key[0]: scene_id_value[0], scene_id_key[1]: scene_id_value[1], ...}
        """
        dag_run_df = await get_dag_runs(
            self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True, flatten_conf=True
        )

        if len(dag_run_df) == 0:
            return []

        existing_scenes = []
        for i, row in dag_run_df.iterrows():
            scn = {k: v for k, v in zip(self.expanded_scene_id_keys, row[self.expanded_scene_id_keys].values)}
            scn["state"] = row["dag_run_state"]
            existing_scenes.append(scn)
        return existing_scenes
