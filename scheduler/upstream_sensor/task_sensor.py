import pandas as pd

from ..helpers.airflow_api import get_dag_runs, get_task_instance
from .base import UpstreamSensor


class TaskSensor(UpstreamSensor):
    def __init__(
        self, api_url: str, batch_id: str, cookies: dict, dag_id: str = None, task_id: str = None, **kwargs
    ) -> None:
        super().__init__()
        self.api_url = api_url
        self.batch_id = batch_id
        self.dag_id = dag_id
        self.task_id = task_id
        self.cookies = cookies

    async def sense(self, state: str = None) -> pd.DataFrame:
        dag_run_df = await get_dag_runs(self.api_url, self.batch_id, self.dag_id, self.cookies, to_dataframe=True)

        if len(dag_run_df) == 0:
            return pd.DataFrame([])

        task_instances = []
        for dag_run_id in dag_run_df["dag_run_id"]:
            task_instances.append(
                await get_task_instance(
                    self.api_url, self.dag_id, dag_run_id, self.task_id, self.cookies, to_dataframe=True
                )
            )
        task_instance_df = pd.concat(task_instances).reset_index(drop=True)

        assert len(dag_run_df) == len(task_instance_df), "#taskInstances should match #dagRuns"

        status_df = pd.merge(dag_run_df, task_instance_df, how="inner", on=["dag_id", "dag_run_id"])
        if state is not None:
            status_df = status_df[status_df.task_instance_state == state].reset_index(drop=True)
        status_df.loc[:, "state"] = status_df.task_instance_state
        # status_df.loc[:, "state"] = status_df.apply(
        #     lambda x: x.dag_run_state if pd.isnull(x.task_instance_state) else x.task_instance_state, axis=1
        # )

        return status_df

    @property
    def query_key_values(self) -> list[str]:
        return {"batch_id": self.batch_id, "dag_id": self.dag_id, "task_id": self.task_id}
