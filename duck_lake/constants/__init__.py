from pathlib import Path

dbt_project_dir = Path(__file__).joinpath("..","..","..","duck_lake_models").resolve()
dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")