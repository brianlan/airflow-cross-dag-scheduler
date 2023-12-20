import aiohttp
from typing import Any, List

from ..helpers import aiohttp_requests as aio

class WatchResult:
    def __init__(self) -> None:
        self.action = "unset"
        self.context = {}
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "action" and __value not in ["trigger", "unset", "error", "check"]:
            raise ValueError("Invalid action")


class BaseWatcher:
    async def run(self):
        while True:
            result = await self.watch()
            if result.action == "trigger":
                await self.trigger()
    
    async def watch(self) -> WatchResult:
        raise NotImplementedError

    async def trigger(self) -> None:
        raise NotImplementedError


class RestAPIWatcher(BaseWatcher):
    def __init__(self, api_url: str, dag_id: str, upstream: List[dict], cookie_session_path: str) -> None:
        """__init__ of RestAPIWatcher

        Parameters
        ----------
        api_url : str
            api endpoint url
        dag_id : str
            the dag_id that the watcher is watching
        upstream : List[dict]
            descriptions of the upstream dag / task
            For example: 
            upstream = [
                {"dag_id": "generate_base_data", "task_id": "generate_image"},
                {"dag_id": "optimize_ipm"},
                {"dag_id": "fisheye_segmentation", "task_group_id": "tg_nvidia_segmentation"}
            ]
            For each item in `upstream`, if task_id provided, this upstream is considered as success only when the task is success.
            If task_id is not provided, this upstream is considered as success when the dag is success.
        """
        self.api_url = api_url
        self.dag_id = dag_id
        self.upstream = upstream
        self.cookies = {"session": self.read_cookie_session(cookie_session_path)}

    def read_cookie_session(self, path):
        with open(path, 'r') as f:
            return f.read().strip()

    async def get_dagruns(self, dag_id: str, get_taskinstance: bool = False) -> list:
        """Get all the DagRuns of `dag_id` using Airflow RESTAPI:
        http://{api_url}/api/v1/dags/{dag_id}/dagRuns

        Parameters
        ----------
        dag_id : str
            dag id
        get_taskinstance : bool, optional
            if True, will query the taskinstance state for each running DagRun, by default False.

        Returns
        -------
        list
            list of dag run info
        """
        url = f"{self.api_url}/api/v1/dags/{dag_id}/dagRuns"
        resp_json = await aio.get(url, cookies=self.cookies)
        dagruns = resp_json["dag_runs"]
        if get_taskinstance:
            for dagrun in dagruns:
                dagrun['task_instances'] = await self.get_taskinstances(dag_id, dagrun['dag_run_id'])
        return dagruns

    async def get_taskinstances(self, dag_id: str, dag_run_id: str) -> list:
        """Get all the taskinstance status of a DagRun using Airflow RestAPI:
        http://{api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances

        Parameters
        ----------
        dag_id : str
            dag id
        dag_run_id : str
            dagRun id

        Returns
        -------
        list
            list of taskinstance info
        """
        url = f"{self.api_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        resp_json = await aio.get(url, cookies=self.cookies)
        return resp_json["task_instances"]

    async def get_tasks(self, dag_id: str) -> List[dict]:
        """Get static task definitions of a given dag_id

        Parameters
        ----------
        dag_id : str
            dag id

        Returns
        -------
        List[dict]
            _description_
        """
        url = f"{self.api_url}/api/v1/dags/{dag_id}/tasks"
        resp_json = await aio.get(url, cookies=self.cookies)
        tasks = resp_json["tasks"]
        return tasks

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
            payload['dag_run_id'] = dag_run_id
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    # Handle error
                    pass

    async def all_upstream_success(self, job_definition: dict) -> bool:
        """Check whether all the upstreams are success for the keys definition

        Parameters
        ----------
        job_definition : dict
            A series of key-value pairs that represent the upstream dags.
            For example:
            job_definition = {"scene_id": "20131101_scene", "split_id": "0"}

        Returns
        -------
        bool
            _description_
        """
        for upstream_dag in self.upstream:
            dag_id = upstream_dag['dag_id']
            dag_runs = await self.get_dagruns(dag_id, get_taskinstance=True)
            # Add logic to check if any dag run matches job_definition and is successful
            # This will depend on the structure of your job_definition and the response from Airflow
        return True  # If all checks are passed

    async def is_upstream_dag_success(self, dag_id: str, dag_run_id: str, job_definition: dict) -> bool:
        pass

    async def is_upstream_task_success(self, dag_id: str, dag_run_id: str, task_id: str, job_definition: dict) -> bool:
        pass

    async def is_upstream_task_group_success(self, dag_id: str, dag_run_id: str, task_group_id: str, job_definition: dict) -> bool:
        pass

    async def get_all_ready_job_definitions(self) -> List[dict]:
        """Get all the ready job_definitions

        Returns
        -------
        List[dict]
            list of upstream ready conf
        """
        pass
