# airflow-cross-dag-scheduler
airflow-cross-dag-scheduler

### Important Notes:
dagrun.conf must contain the following keywords:
- batch_id
- scene_id

### Structures

batch_id:baidu
  └── dag_id:generate_base_data
         └── scene_id:20231101_1642 ─── dag_run_id:Manual_2023xxxxxx
                                                ├── task_id:generate_image
                                                └── task_id:generate_lidar
