from typing import Any
import asyncio
import importlib
import traceback

from loguru import logger

from ..upstream_sensor.base import create_sensor


class WatchResult:
    def __init__(self) -> None:
        self.action = "unset"
        self.context = {}

    # def __setattr__(self, __name: str, __value: Any) -> None:
    #     if __name == "action" and __value not in ["trigger", "unset", "error", "watch"]:
    #         raise ValueError("Invalid action")
    
    def __repr__(self) -> str:
        return f"WatchResult(action={self.action}, context={self.context})"


class BaseWatcher:
    def __init__(self, watch_interval: int = 10) -> None:
        """
        Parameters
        ----------
        watch_interval : int
            time interval (in seconds) between each watch, by default 10
        """
        self.watch_interval = watch_interval

    async def run(self):
        while True:
            _dag_id = getattr(self, 'dag_id', None)
            try:
                result = await self.watch()
                logger.info(f"[Watcher {_dag_id}] Watch result: {result}")
                if result.action == "trigger":
                    await self.trigger(result.context)
            except Exception as e:
                logger.error(f"[Watcher {_dag_id}] err_msg: {e}")
                traceback.print_exc()

            # sleep for some time
            await asyncio.sleep(self.watch_interval)

    async def watch(self) -> WatchResult:
        raise NotImplementedError

    async def trigger(self, context: dict) -> None:
        raise NotImplementedError


def create_watcher(api_url: str, batch_id: str, cookies: dict, wcfg: dict):
    module, cls = wcfg.pop("class").rsplit(".", 1)
    watcher_cls = getattr(importlib.import_module(module), cls)
    upstream = wcfg.pop("upstream")
    watcher = watcher_cls(
        api_url,
        batch_id,
        cookies,
        [create_sensor(api_url, batch_id, cookies, s) for s in upstream],
        **wcfg,
    )
    return watcher
