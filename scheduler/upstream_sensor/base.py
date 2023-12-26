from typing import Any, List
import importlib


class UpstreamSensor:
    async def sense(self, state: str = None) -> Any:
        """sense the state of the upstream

        Parameters
        ----------
        state : str, optional
            if provided will return the ones that has this state, by default None

        Returns
        -------
        Any
            _description_

        Raises
        ------
        NotImplementedError
            _description_
        """
        raise NotImplementedError

    @property
    def query_key_values(self) -> List[str]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return str(self.query_key_values)


def create_sensor(api_url, batch_id, cookies, scfg) -> UpstreamSensor:
    module, cls = scfg.pop("class").rsplit(".", 1)
    sensor_cls = getattr(importlib.import_module(module), cls)
    sensor = sensor_cls(api_url, batch_id, cookies, **scfg["args"])
    return sensor
