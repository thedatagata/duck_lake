import os 
from pathlib import Path
from .assets import clean_source_data_assets, load_source_data_assets 
from .assets.lake_models import dbt_model_assets
from .jobs import load_data_job

from dagster_gcp.gcs import GCSResource, GCSPickleIOManager
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt import DbtCliResource

from dagster import Definitions, EnvVar

dbt_project_dir = Path(__file__).joinpath("..", "dbt_project").resolve() 
dbt_project_path = os.fspath(dbt_project_dir)
data_warehouse_dir = Path(__file__).joinpath("..", "data_warehouse").resolve() 

defs = Definitions(
    assets=[*clean_source_data_assets, *load_source_data_assets, dbt_model_assets],
    resources={
        "gcs": GCSResource(EnvVar("GOOGLE_CLOUD_PROJECT")),
        "gcs_io": GCSPickleIOManager(gcs_bucket="pond_water", gcs_prefix="processed_ga_dumps"),
        "db_io": DuckDBPandasIOManager(database=data_warehouse_dir.joinpath("duck_lake.db")),
        "dbt":DbtCliResource(dbt_project_path=dbt_project_path)
    },
    jobs=[load_data_job]
)