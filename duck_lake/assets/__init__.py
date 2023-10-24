from dagster import load_assets_from_package_module

from . import filter_swamp_water, fill_duck_lake

clean_source_data_assets = load_assets_from_package_module(
    package_module=filter_swamp_water, 
    group_name="clean_raw_data"
) 

load_source_data_assets = load_assets_from_package_module(
    package_module=fill_duck_lake,
    group_name="load_raw_data"

)