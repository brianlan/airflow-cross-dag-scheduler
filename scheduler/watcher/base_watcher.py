import asyncio
from dataclasses import dataclass
from typing import Any


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
    def __init__(self, api_url: str, dag_id: str, upstream: dict) -> None:
        self.api_url = api_url
        self.dag_id = dag_id
        self.upstream = upstream

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
        pass

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
        pass

    async def trigger_dag(self, dag_id: str, dag_conf: dict = None, dag_run_id: str = None) -> None:
        dag_conf = dag_conf or {}

    async def all_upstream_ready(self) -> bool:
        pass