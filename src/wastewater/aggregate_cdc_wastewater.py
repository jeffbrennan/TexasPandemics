import pandas as pd
import src.utils

def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    def weighted_average(df, data_col):
        # https://stackoverflow.com/questions/31521027/groupby-weighted-average-and-sum-in-pandas-dataframe/31521177#31521177
        weight_col = 'population_served'
        by_col = ['County', 'Date']

        df['_data_times_weight'] = df[data_col] * df[weight_col]
        df['_weight_where_notnull'] = df[weight_col] * pd.notnull(df[data_col])
        g = df.groupby(by_col)
        result = g['_data_times_weight'].sum() / g['_weight_where_notnull'].sum()
        del df['_data_times_weight'], df['_weight_where_notnull']
        return result

    ww_metrics = ['normalized_levels_15d', 'normalized_levels_pct_difference_15d',
                  'pct_samples_with_detectable_levels_15d']
    aggregations = {metric: weighted_average(df, metric) for metric in ww_metrics}

    aggregated_df = (
        pd.DataFrame.from_records(aggregations)
        .reset_index()
    )

    return aggregated_df


def run_diagnostics(df: pd.DataFrame) -> None:
    check_no_nulls = df.isnull().sum().sum()
    assert check_no_nulls == 0, 'Null values found'


# region setup --------------------------------------------------------------------------------
current_agg_df = src.utils.load_csv('tableau/wastewater/cdc_wastewater_aggregated.csv')
clean_df = src.utils.load_csv('tableau/wastewater/cdc_wastewater.csv')
# endregion

# region calc + combine --------------------------------------------------------------------------------
aggregated_df = aggregate_data(clean_df)

aggregate_df_out = pd.concat([current_agg_df, aggregated_df]).drop_duplicates()
aggregate_df_out = aggregated_df
# endregion

# region  --------------------------------------------------------------------------------
run_diagnostics(aggregate_df_out)
src.utils.write_file(aggregate_df_out, 'tableau/wastewater/cdc_wastewater_aggregated')
# endregion
