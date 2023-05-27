from pathlib import Path

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    Definitions,
    load_assets_from_modules,
)

from . import assets


from datetime import datetime as dt


class PandasManager(ConfigurableIOManager):

    @staticmethod
    def read_dataframe_from_disk(schema: str, table_name: str) -> pd.DataFrame:
        df = pd.read_csv(f'data/{schema}/{table_name}.csv')
        return df

    @staticmethod
    def write_dataframe_to_disk(schema: str, table_name: str, add_archive: bool, dataframe: pd.DataFrame) -> None:
        base_dir = f'data/{schema}'
        Path(base_dir).mkdir(parents=True, exist_ok=True)

        if add_archive:
            Path(f'{base_dir}/archive').mkdir(parents=True, exist_ok=True)
            dataframe.to_csv(f'{base_dir}/archive/{table_name}_{dt.now().strftime("%Y-%m-%d")}.csv', index=False)

        dataframe.to_csv(f'{base_dir}/{table_name}.csv', index=False)

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        self.write_dataframe_to_disk(
            schema=context.metadata['schema'],
            table_name=context.metadata['table_name'],
            add_archive=context.metadata['add_archive'],
            dataframe=obj
        )

    def load_input(self, context) -> pd.DataFrame:
        table_name = context.upstream_output.metadata['table_name']
        schema = context.upstream_output.metadata['schema']
        df = self.read_dataframe_from_disk(schema=schema, table_name=table_name)
        return df


resources = {"pandas_io_manager": PandasManager()}
defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)
