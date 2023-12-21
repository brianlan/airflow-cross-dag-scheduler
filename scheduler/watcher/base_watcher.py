from typing import Any


class WatchResult:
    def __init__(self) -> None:
        self.action = "unset"
        self.context = {}
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "action" and __value not in ["trigger", "unset", "error", "watch"]:
            raise ValueError("Invalid action")


class BaseWatcher:
    async def run(self):
        while True:
            result = await self.watch()
            if result.action == "trigger":
                await self.trigger(result.context)
    
    async def watch(self) -> WatchResult:
        raise NotImplementedError

    async def trigger(self, context: dict) -> None:
        raise NotImplementedError


class BatchEntry:
    def __init__(self, scene_list: dict) -> None:
        self.scene_list = scene_list
                                                                                                                         