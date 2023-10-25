import os
from .assets import clean_source_data_assets, load_source_data_assets 
from .assets.lake_models import dbt_model_assets 
from .constants import dbt_project_dir
from .jobs import load_data_job 

from dagster_gcp.gcs import GCSResource, GCSPickleIOManager
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt import DbtCliResource

from dagster import Definitions 

print()

defs = Definitions(
    assets=[*clean_source_data_assets, *load_source_data_assets, dbt_model_assets],
    resources={
        "gcs_io": GCSPickleIOManager(
            gcs=GCSResource(project=os.getenv("GOOGLE_CLOUD_PROJECT")), 
            gcs_bucket="pond-water", 
            gcs_prefix="processed_ga_dumps"
        ),
        "db_io": DuckDBPandasIOManager(database="data_warehouse/duck_lake.db"),
        "dbt":DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[load_data_job]
)