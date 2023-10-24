from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

dbt_project_dir = Path(__file__).joinpath("..","..", "..", "dbt_project").resolve()

@dbt_assets(
        manifest=dbt_project_dir.joinpath("target","manifest.json"),
        io_manager_key="db_io_manager"
        )
def dbt_model_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()