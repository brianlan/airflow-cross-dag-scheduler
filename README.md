# airflow-cross-dag-scheduler
airflow-cross-dag-scheduler

## Important Notes:

#### dagrun.conf 
dag_run.conf must contain the following keywords:
- batch_id
- scene_id

#### SceneId
SceneId is not exactly equals to {"scene_id": "20231102_1213"}, it could be multiple key-value pairs that represent an abstract concept of Scene, which is used to connect our different DAGs working together.

### Structures

```
batch_id:baidu
  └── dag_id:generate_base_data
         └── scene_id:20231101_1642 ─── dag_run_id:Manual_2023xxxxxx
                                                ├── task_id:generate_image
                                                └── task_id:generate_lidar
```