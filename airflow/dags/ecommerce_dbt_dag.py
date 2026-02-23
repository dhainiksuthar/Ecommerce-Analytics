from airflow import DAG
from cosmos import ProjectConfig, ProfileConfig
from cosmos.operators import DbtRunLocalOperator, DbtTestLocalOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_analytics_dbt",
    start_date=datetime(2025, 1, 1),
    schedule="0 6,18 * * *",
    catchup=False,
):
    project_config = ProjectConfig(
        dbt_project_path="/dbt_project",
    )

    profile_config = ProfileConfig(
        profile_name="ecommerce_analytics",
        target_name="dev",
        profiles_yml_filepath="/dbt_project/profiles.yml",
    )

    dbt_run = DbtRunLocalOperator(
        task_id="dbt_run",
        project_dir="/dbt_project",
        profile_config=profile_config,
        install_deps=True,
    )

    dbt_test = DbtTestLocalOperator(
        task_id="dbt_test",
        project_dir="/dbt_project",
        profile_config=profile_config,
    )

    dbt_run >> dbt_test
