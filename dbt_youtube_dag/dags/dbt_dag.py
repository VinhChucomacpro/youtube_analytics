from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode
import sys
import os

os.environ['AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT'] = '120'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
from youtube_crawler import crawl_youtube_data

# PATHS
dbt_project_path = "/usr/local/airflow/dags/dbt/dbt_youtube"
profiles_path = "/usr/local/airflow/dags/dbt/dbt_youtube"
manifest_path = f"{dbt_project_path}/target/manifest.json"

# DBT CONFIG với manifest_path
project_config = ProjectConfig(
    dbt_project_path=dbt_project_path,
    manifest_path=manifest_path,  # ✅ Manifest path ở đây
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

profile_config = ProfileConfig(
    profile_name="dbt_youtube",
    target_name="dev",
    profiles_yml_filepath=f"{profiles_path}/profiles.yml",
)

# WRAPPER
def crawl_wrapper(**_):
    return crawl_youtube_data(use_cdc=True, crawl_comments=True)

# DAG
with DAG(
    dag_id="dbt_youtube_pipeline_with_cdc",
    start_date=datetime(2025, 9, 20),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "dbt_youtube", "cdc", "comments"],
) as dag:

    task_crawl = PythonOperator(
        task_id="crawl_data_from_api_with_cdc",
        python_callable=crawl_wrapper,
    )

    # ✅ RenderConfig KHÔNG CẦN manifest_path
    seeds = DbtTaskGroup(
        group_id="seeds",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["resource_type:seed"],
        ),
        default_args={"retries": 2},
    )

    bronze = DbtTaskGroup(
        group_id="bronze",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["tag:bronze"],
        ),
        default_args={"retries": 2},
    )

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["tag:silver"],
        ),
        default_args={"retries": 2},
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["tag:gold"],
        ),
        default_args={"retries": 2},
    )

    task_crawl >> seeds >> bronze >> silver >> gold