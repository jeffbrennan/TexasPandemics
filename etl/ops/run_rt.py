from dagster import op
from src.utils import run_r_script


@op
def compute_rt() -> bool:
    config = {
        'script_path': "src/rt/compute_rt.r",
        'arg': "--vanilla",
        'input_file': "data/tableau/county_vitals.parquet",
        'output_file': "data/intermediate/rt/rt_all.parquet"
    }

    success = run_r_script(config)
    return success


compute_rt()
