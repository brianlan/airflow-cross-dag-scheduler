from typing import List

import pandas as pd
from loguru import logger

from ..helpers.base import is_in_df
from ..helpers.airflow_api import get_dag_runs, trigger_dag
from ..upstream_sensor.base import UpstreamSensor
from .base import BaseWatcher, WatchResult


class RestAPIWatcher(BaseWatcher):
    def __init__(
        self,
        api_url: str,
        batch_id: str,
        cookies: dict,
        upstream_sensors: List[UpstreamSensor],
        *,
        dag_id: str = None,
        fixed_dag_run_conf: dict = None,
        scene_id_keys: List[str] = None,
        max_running_dag_runs: int = 3,
        use_scene_id_keys_as_dag_run_id: bool = False,
        watch_interval: int = 10,
    ) -> None:
        """__init__ of RestAPIWatcher

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
        cookie_session_path : str
            the path to the cookie_session file that required by Airflow REST API for authentication
        max_running_dag_runs : int, optional
            the maximum number of running dag_runs that the watcher will keep, by default 3
        use_scene_id_keys_as_dag_run_id : bool, optional
            if True, will use scene_id_keys as dag_run_id when triggering dag, by default False
        watch_interval : int
            time interval (in seconds) between each watch, by default 10
        """
        super().__init__(watch_interval=watch_interval)

        # check the input's validity
        assert len(scene_id_keys) > 0, "scene_id_keys should not be empty"
        # assert len(upstream_sensors) == len(
        #     {u.dag_id for u in upstream_sensors}
        # ), "the same dag_id appears more than once in upstream definition."

        self.batch_id = batch_id
        self.api_url = api_url
        self.dag_id = dag_id
        self.scene_id_keys = scene_id_keys
        self.fixed_dag_run_conf = fixed_dag_run_conf
        self.upstream_sensors = upstream_sensors
        self.max_running_dag_runs = max_running_dag_runs
        self.use_scene_id_keys_as_dag_run_id = use_scene_id_keys_as_dag_run_id
        self.cookies = cookies

    def __repr__(self) -> str:
        return f"RestAPIWatcher({self.dag_id})"

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
            has_been_triggered = [e for e in existing_scenes if all(e[k] == ready_scene[k] for k in self.scene_id_keys)]
            if len(has_been_triggered) == 0 and trigger_quota > 0:
                result.action = "trigger"
                result.context = ready_scene
                return result
        
        return result

    async def trigger(self, context: dict) -> None:
        dag_conf = {
            "batch_id": self.batch_id,
            **context,
            **self.fixed_dag_run_conf,
        }
        dag_run_id = None
        if self.use_scene_id_keys_as_dag_run_id:
            dag_run_id = "_".join([f"{k}:{v}" for k, v in context.items()])
        status, json_data = await trigger_dag(self.api_url, self.dag_id, self.cookies, dag_conf=dag_conf, dag_run_id=dag_run_id)
        logger.info(f"[Watcher {self.dag_id}] Triggered DAG.")
        logger.info(f"[Watcher {self.dag_id}] Response from Airflow {json_data}")

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

        for skeys, subdf in success_df.groupby(self.scene_id_keys):
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
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True)

        if len(dag_run_df) == 0:
            return []

        # extract scene_id_keys, such as 'scene_id' out of the `conf` column
        for key in self.scene_id_keys:
            if key not in dag_run_df.columns:
                dag_run_df.loc[:, key] = dag_run_df.conf.map(lambda x: x.get(key))
        existing_scenes = []
        for i, row in dag_run_df.iterrows():
            scn = {k: v for k, v in zip(self.scene_id_keys, row[self.scene_id_keys].values)}
            scn["state"] = row["dag_run_state"]
            existing_scenes.append(scn)
        return existing_scenes
