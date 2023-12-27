from typing import List
import time

import pandas as pd
from loguru import logger

from ..helpers.base import is_in_df
from ..helpers.airflow_api import get_dag_runs
from .restapi_watcher import RestAPIWatcher
from ..upstream_sensor.xcom_query import XComQuery
from .base import WatchResult


class ReducibleRestAPIWatcher(RestAPIWatcher):
    def __init__(
        self,
        *args,
        reduce_by: XComQuery = None,
        **kwargs,
    ) -> None:
        """__init__ of ReducibleRestAPIWatcher

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
        reduce_by: XComQuery
            description to how to get the xcom_values to reduce the dag_run.
            For example:
                reduce_by = {
                    "dag_id": "dag_split_map_generator",
                    "task_id": "generate_split_map",
                    "xcom_key": "return_value",
                    "refer_name": "split_id"
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
        self.reduce_by = reduce_by

    def __repr__(self) -> str:
        return f"ReducibleRestAPIWatcher({self.dag_id}, reduce_by={self.reduce_by})"

    @property
    def reduced_scene_id_keys(self) -> List[str]:
        if self.reduce_by:
            idx = self.scene_id_keys.index(self.reduce_by.refer_name)
            return self.scene_id_keys[:idx] + self.scene_id_keys[idx + 1 :]
        return self.scene_id_keys

    async def reduce(self, scenes: List[dict]) -> List[dict]:
        """Reduce the scenes by self.reduce_by"""
        expand_dag_run_df = await self.reduce_by.query(self.api_url, self.batch_id, self.cookies, state="success")
        
        if len(expand_dag_run_df) == 0:
            return []
        
        df = pd.DataFrame.from_records(scenes)
        merged = pd.merge(df, expand_dag_run_df, how="inner", on=self.scene_id_keys).reset_index(drop=True)
        expanded_scenes = merged.to_dict(orient="records")
        return expanded_scenes

    async def watch(self) -> WatchResult:
        logger.info(f"[Watcher {self.dag_id}] Start watching..")
        ready_scenes = await self.get_all_upstream_ready_scenes()
        existing_scenes = await self.get_existing_scenes()
        running_scenes = [s for s in existing_scenes if s["state"] == "running"]
        trigger_quota = self.max_running_dag_runs - len(running_scenes)
        result = WatchResult()
        if len(ready_scenes) == 0 or trigger_quota <= 0:
            result.action = "watch"
            return result

        # trigger the first scene that meets the requirement
        for ready_scene in ready_scenes:
            has_been_triggered = [e for e in existing_scenes if all(e[k] == ready_scene[k] for k in self.reduced_scene_id_keys)]
            if len(has_been_triggered) == 0 and trigger_quota > 0:
                result.action = "trigger"
                result.context = ready_scene
                return result

        return result

    async def get_all_upstream_ready_scenes(self) -> List[dict]:
        """Get all the ready scene's

        Returns
        -------
        List[dict]
            list of upstream ready conf
        """
        ready_scenes = []

        success_df_list = [await sensor.sense(state="success") for sensor in self.upstream_sensors]
        success_df = pd.concat(success_df_list).reset_index(drop=True)

        if len(success_df) == 0:
            return []

        _scene_id_keys = self.scene_id_keys[0] if len(self.scene_id_keys) == 1 else self.scene_id_keys # to prevent pandas warning
        for skeys, subdf in success_df.groupby(_scene_id_keys):
            if isinstance(skeys, str):
                skeys = [skeys]
            num_success = sum([is_in_df(snr.query_key_values, subdf) for snr in self.upstream_sensors])
            if num_success == len(self.upstream_sensors):
                ready_scenes.append({k: v for k, v in zip(self.scene_id_keys, skeys)})

        return ready_scenes


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
            scn = {k: v for k, v in zip(self.reduced_scene_id_keys, row[self.reduced_scene_id_keys].values)}
            scn["state"] = row["dag_run_state"]
            existing_scenes.append(scn)
        return existing_scenes
