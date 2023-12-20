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
