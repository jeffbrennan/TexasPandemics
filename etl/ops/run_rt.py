import os

import pandas as pd
from dagster import asset, op, AssetIn

from src.utils import run_r_script


# @op(
#     name="compute_rt",
#     description="Computes rt for all counties",
#     ins={
#         "county_vitals":
#             AssetIn(
#                 key=["vitals", "county_vitals"]
#             )
#     },
# ins={"county_vitals": "vitals/county_vitals"}
#
# TODO: figure out how to make this an op
@asset(
    name="rt_all",
    key_prefix=["intermediate", "rt"],
    group_name="intermediate_rt",
    metadata={
        "schema": "intermediate/rt",
        "table_name": "rt_all",
        "add_archive": False
    },
    non_argument_deps={'vitals/county_vitals'},
    io_manager_key="pandas_io_manager"
)
def compute_rt() -> pd.DataFrame:
    config = {
        'script_path': "src/rt/compute_rt.r",
        'arg': "--vanilla",
        'input_file': "data/tableau/county_vitals.parquet",
        'output_file': "data/temp/rt_all.parquet"
    }

    success = run_r_script(config)

    if not success:
        raise Exception("R script failed to run")

    rt_results = pd.read_parquet(config['output_file'])
    os.remove(config['output_file'])
    return rt_results
