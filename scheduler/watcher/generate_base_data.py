import asyncio

from . import BaseWatcher, WatchResult, RestAPIWatcher

class GenerateBaseDataWatcher(BaseWatcher):
    def __init__(self, ) -> None:
        super().__init__()

    async def watch(self) -> WatchResult:
        pass

    async def run(self) -> None:
        while True:
            # Process the data
            await asyncio.sleep(1)