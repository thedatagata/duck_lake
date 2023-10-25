from pathlib import Path

from ..assets import clean_source_data_assets, load_source_data_assets

from dagster import define_asset_job

load_data_config = {
    "execution": {
        "config": {
            "multiprocess":{
                "max_concurrent": 1
            }
        }
    },
    "ops":{
        "process_swamp_water":{
            "ops":{
                "get_swamp_water":{
                    "config":{
                        "ga_dump_file": "ga_data_20180401.csv"
                    }
                }
            }
        }
    }
}

load_data_job = define_asset_job(
    "load_data_job", 
    selection=[*clean_source_data_assets, *load_source_data_assets],
    config=load_data_config
)

