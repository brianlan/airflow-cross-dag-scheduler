from typing import List

import pandas as pd

from ..helpers.airflow_api import get_dag_runs
from ..helpers import aiohttp_requests as ar
from ..upstream_sensor.base import UpstreamSensor
from ..upstream_sensor.dag_sensor import DagSensor
from .base_watcher import BaseWatcher, WatchResult


class RestAPIWatcher(BaseWatcher):
    def __init__(
        self,
        api_url: str,
        batch_id: str,
        dag_id: str,
        fixed_dag_run_conf: dict,
        upstream_sensors: List[UpstreamSensor],
        scene_id_keys: List[str],
        cookie_session_path: str,
        max_running_dag_runs: int = 3,
        use_scene_id_keys_as_dag_run_id: bool = False,
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
        """
        # calls the __init__ of BaseWatcher
        super().__init__()

        # check the input's validity
        assert len(scene_id_keys) > 0, "scene_id_keys should not be empty"
        assert len(upstream_sensors) == len(
            {u.dag_id for u in upstream_sensors}
        ), "the same dag_id appears more than once in upstream definition."

        self.batch_id = batch_id
        self.api_url = api_url
        self.dag_id = dag_id
        self.scene_id_keys = scene_id_keys
        self.fixed_dag_run_conf = fixed_dag_run_conf
        self.upstream_sensors = upstream_sensors
        self.max_running_dag_runs = max_running_dag_runs
        self.use_scene_id_keys_as_dag_run_id = use_scene_id_keys_as_dag_run_id
        self.cookies = {"session": self.read_cookie_session(cookie_session_path)}

    def read_cookie_session(self, path):
        with open(path, "r") as f:
            return f.read().strip()

    async def watch(self) -> WatchResult:
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
            if ready_scene not in existing_scenes and trigger_quota > 0:
                result.action = "trigger"
                result.context = ready_scene
                return result

    async def trigger(self, context: dict) -> None:
        dag_conf = {
            "batch_id": self.batch_id,
            **context["scene_id_keys"].items(),
        }
        dag_run_id = None
        if self.use_scene_id_keys_as_dag_run_id:
            dag_run_id = "_".join([f"{k}:{v}" for k, v in context["scene_id_keys"].items()])
        status, json_data = await self.trigger_dag(self.dag_id, dag_conf=dag_conf, dag_run_id=dag_run_id)

    async def trigger_dag(self, dag_id: str, dag_conf: dict = None, dag_run_id: str = None) -> None:
        """Trigger a DagRun using Airflow RestAPI:
        https://{api_url}/api/v1/dags/{dag_id}/dagRuns

        Parameters
        ----------
        dag_id : str
            dag id
        dag_conf : dict, optional
            conf dict that passed to DagRun when triggering, by default None
        dag_run_id : str, optional
            if specified, will use this as DagRunId, by default None
        """
        dag_conf = dag_conf or {}
        url = f"{self.api_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {"conf": dag_conf}
        if dag_run_id:
            payload["dag_run_id"] = dag_run_id
        return await ar.post(url, payload, cookies=self.cookies)

    async def get_all_upstream_status(self) -> pd.DataFrame:
        status_df_list = [await sensor.sense() for sensor in self.upstream_sensors]
        return pd.concat(status_df_list).reset_index(drop=True)

    async def get_all_upstream_ready_scenes(self) -> List[dict]:
        """Get all the ready scene's

        Returns
        -------
        List[dict]
            list of upstream ready conf
        """
        ready_scenes = []
        status_df = await self.get_all_upstream_status()
        for skeys, subdf in status_df.groupby(self.scene_id_keys):
            if isinstance(skeys, str):
                skeys = [skeys]
            deps = []
            for sensor in self.upstream_sensors:
                if isinstance(sensor, DagSensor):
                    dep_dag_run = subdf[subdf.dag_id == sensor.dag_id]
                    assert len(dep_dag_run) <= 1, f"{sensor.dag_id} should have only one dag_run that matches {skeys}"
                    if len(dep_dag_run) == 1 and dep_dag_run.state.iloc[0] == "success":
                        deps.append(dep_dag_run.iloc[0].to_dict())
                    continue
                dep_task_instance = subdf[(subdf.dag_id == sensor.dag_id) & (subdf.task_id == sensor.task_id)]
                assert (
                    len(dep_task_instance) <= 1
                ), f"{sensor.dag_id} should have only one task_instance {sensor.task_id} that matches {skeys}"
                if len(dep_task_instance) == 1 and dep_task_instance.state.iloc[0] == "success":
                    deps.append(dep_task_instance.iloc[0].to_dict())
            if len(deps) == len(self.upstream_sensors):
                ready_scenes.append(
                    {"scene_id_keys": {k: v for k, v in zip(self.scene_id_keys, skeys)}, "upstream": deps}
                )
        return ready_scenes

    async def get_existing_scenes(self) -> List[dict]:
        """Get all the existing scenes of self.dag_id

        Returns
        -------
        List[dict]
            list of existing scenes, each scene is a dict of {scene_id_key[0]: scene_id_value[0], scene_id_key[1]: scene_id_value[1], ...}
        """
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, scene_id_keys=self.scene_id_keys, to_dataframe=True)
        existing_scenes = []
        for i, row in dag_run_df.iterrows():
            scn = {k: v for k, v in zip(self.scene_id_keys, row[self.scene_id_keys].values)}
            scn["state"] = row["dag_run_state"]
            existing_scenes.append(scn)
        return existing_scenes
