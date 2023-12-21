from typing import Any


class UpstreamSensor:
    async def sense(self) -> Any:
        raise NotImplementedError