import pandas as pd
import plotly.io as pio
import plotly.graph_objects as go
from datetime import timedelta

pio.renderers.default = "browser"
from plotly.subplots import make_subplots
from src.utils import filter_df_date


def get_clean_rt() -> pd.DataFrame:
    # def save_rt_file(df):

    def format_rt_file(df: pd.DataFrame):
        clean_df = (
            df.query("Level == 'TMC'")
            .query('~Rt.isna()')
            [['Date', 'Rt', 'lower', 'upper']]
        )
        return clean_df

    def get_rt_file():
        return pd.read_parquet('data/intermediate/rt/rt_all.parquet')

    tmc_rt_raw = get_rt_file()
    tmc_rt = format_rt_file(tmc_rt_raw)
    return tmc_rt


def county_cases_plot(county_cases: pd.DataFrame, case_summary: pd.DataFrame):
    def update_layout(fig, case_summary):
        y_min = 0
        cases_daily_max = case_summary['cases_daily'].max()
        ma7_max = case_summary['ma_7day'].max()
        y_max = max(cases_daily_max, ma7_max) * 1.1

        fig.update_layout(barmode='stack')
        fig.update_yaxes(title_text="7-day MA", secondary_y=False, range=[y_min, y_max])
        fig.update_yaxes(title_text="Daily Cases", secondary_y=True, range=[y_min, y_max])
        fig.update_layout(xaxis=dict(tickformat="%m/%d"))
        fig.update_layout(plot_bgcolor='white')
        fig.update_yaxes(visible=False)
        fig.update_xaxes(tickmode='linear')
        # add title
        fig.update_layout(
            title_text="TMC County Cases (Past 2 Weeks) ",
            title_font_size=24,
            title_font_color='black',
            title_font_family='Arial'
        )
        return fig

    def create_case_labels(df):
        case_total_text = go.Scatter(
            x=df.index,
            y=df['cases_daily'],
            text=df['cases_daily'],
            mode='text',
            textposition='top center',
            textfont=dict(
                size=18,
            ),
            showlegend=False,
        )
        return case_total_text

    def create_county_bar_plot(df: pd.DataFrame, county: str):
        county_colors = {
            'Harris': '#6748BC',
            'Galveston': '#00C9A7',
        }

        bar_plot = go.Bar(
            x=df.query("County == @county")['Date'],
            y=df.query("County == @county")['cases_daily'],
            name=county,
            marker_color=county_colors[county]
        )
        return bar_plot

    def create_moving_average_plot(df):
        scatter_plot = go.Scatter(
            x=df.index,
            y=df['ma_7day'],
            mode='lines+markers',
            line=dict(
                color='black',
                width=2,
            ),
            name='7-day MA',
            marker=dict(
                color='black',
                size=10
            ),
            showlegend=False,
        )
        return scatter_plot

    case_labels = create_case_labels(case_summary)
    ma_plot = create_moving_average_plot(case_summary)

    fig = make_subplots(specs=[[{"secondary_y": True}]], shared_yaxes=True)
    fig.add_trace(case_labels)
    fig.add_trace(ma_plot, secondary_y=True)
    fig.add_trace(create_county_bar_plot(county_cases, 'Harris'), secondary_y=True)
    fig.add_trace(create_county_bar_plot(county_cases, 'Galveston'), secondary_y=True)
    final_fig = update_layout(fig, case_summary)
    return final_fig


def rt_estimate_plot(df: pd.DataFrame, plot_type: str = "test") -> go.Figure:
    rt_line = go.Scatter(
        name='Measurement',
        x=df['Date'],
        y=df['Rt'],
        mode='lines+markers',
        line=dict(color='grey', width=2),
        marker=dict(color='black', size=10),
        showlegend=False
    )

    rt_upper = go.Scatter(
        name='Upper Bound',
        x=df['Date'],
        y=df['upper'],
        mode='lines',
        marker=dict(color="#444"),
        line=dict(width=0),
        showlegend=False
    )

    rt_lower = go.Scatter(
        name='Lower Bound',
        x=df['Date'],
        y=df['lower'],
        marker=dict(color="#444"),
        line=dict(width=0),
        mode='lines',
        fillcolor='rgba(68, 68, 68, 0.2)',
        fill='tonexty',
        showlegend=False
    )
    rt_threshold = go.Scatter(
        name='threshold',
        x=df['Date'],
        y=pd.Series(1).repeat(len(df)),
        mode='lines',
        line=dict(color='blue', width=1, dash='dash'),
        showlegend=False)

    def set_xaxis(fig, plot_type):
        if plot_type == 'Past 2 Weeks':
            fig.update_layout(xaxis=dict(tickformat="%m/%d"))
            fig.update_xaxes(tickmode='linear')
        return fig

    fig = go.Figure(
        [rt_line, rt_upper, rt_lower, rt_threshold],
    )
    fig.update_layout(plot_bgcolor='white')
    fig = set_xaxis(fig, plot_type)
    fig.update_layout(
        title_text=f"TMC Rt Estimate ({plot_type})",
        title_font_size=24,
        title_font_color='black',
        title_font_family='Arial'
    )

    return fig


def get_case_dfs() -> list[pd.DataFrame, pd.DataFrame]:
    county_vitals = pd.read_parquet('data/tableau/county_vitals.parquet')
    tmc_county_vitals = county_vitals.query("County in @TMC_COUNTIES")
    max_date = tmc_county_vitals['Date'].max()

    N_DAYS_AGO = 14

    min_date = max_date - timedelta(days=N_DAYS_AGO)

    tmc_county_cases = (
        tmc_county_vitals
        .query("Date >= @min_date")
        [['Date', 'County', 'cases_daily']]
    )

    tmc_summary_prepped = (
        tmc_county_vitals
        .groupby(['Date'])[["cases_daily"]]
        .sum()
        .reset_index()
        .assign(ma_7day=lambda x: x['cases_daily'].rolling(window=7).mean())
    )

    return tmc_county_cases, tmc_summary_prepped


# TODO: figure out how to set layout and styling for each subplot
def create_viz(county_cases, rt_2_weeks, rt_2_months):
    combined_fig = make_subplots(
        rows=2, cols=2,
        specs=[[{'colspan': 2}, None], [{}, {}]],
    )

    for trace in county_cases.data:
        combined_fig.add_trace(trace, row=1, col=1)

    for trace in rt_2_weeks.data:
        combined_fig.add_trace(trace, row=2, col=1)

    for trace in rt_2_months.data:
        combined_fig.add_trace(trace, row=2, col=2)

    combined_fig.update_layout(height=1000, width=1500, title_text="Combined Figure")
    return combined_fig


def save_figure(fig: go.Figure, path: str) -> None:
    fig.write_image(path, width=1500, height=1000, scale=2)


def main():
    tmc_rt = get_clean_rt()

    tmc_county_cases, tmc_summary_prepped = get_case_dfs()

    rt_2_weeks = rt_estimate_plot(
        filter_df_date(tmc_rt, 14),
        plot_type="Past 2 Weeks"
    )

    rt_2_months = rt_estimate_plot(
        filter_df_date(tmc_rt, 60),
        plot_type="Past 2 Months"
    )

    county_cases = county_cases_plot(tmc_county_cases, tmc_summary_prepped)
    combined_viz = create_viz(county_cases, rt_2_weeks, rt_2_months)

    # output
    tmc_rt.to_csv('data/tmc/rt.csv', index=False, lineterminator='\r\n')
    save_figure(combined_viz, 'data/tmc/stacked_plot.png')
