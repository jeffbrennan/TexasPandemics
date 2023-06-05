from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from src.utils import filter_df_date

pio.renderers.default = "browser"


def get_clean_rt() -> pd.DataFrame:
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
            name='7-Day Moving Average',
            marker=dict(
                color='black',
                size=10
            ),
            showlegend=True,
        )
        return scatter_plot

    case_labels = create_case_labels(case_summary)
    ma_plot = create_moving_average_plot(case_summary)

    fig = make_subplots(specs=[[{"secondary_y": True}]], shared_yaxes=True)
    fig.add_trace(case_labels)
    fig.add_trace(create_county_bar_plot(county_cases, 'Harris'), secondary_y=True)
    fig.add_trace(create_county_bar_plot(county_cases, 'Galveston'), secondary_y=True)
    fig.add_trace(ma_plot, secondary_y=True)
    return fig


def rt_estimate_plot(df: pd.DataFrame) -> go.Figure:
    rt_line = go.Scatter(
        name='Measurement',
        x=df['Date'],
        y=df['Rt'],
        mode='lines+markers',
        line=dict(color='grey', width=2),
        marker=dict(color='black', size=6),
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

    fig = go.Figure(
        [rt_line, rt_upper, rt_lower, rt_threshold],
    )

    return fig


def get_case_dfs() -> list[pd.DataFrame, pd.DataFrame]:
    TMC_COUNTIES = ['Harris', 'Galveston']
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
        .query("Date >= @min_date")
        .set_index('Date')
    )

    return tmc_county_cases, tmc_summary_prepped


def create_viz(county_cases: go.Figure, rt_2_weeks: go.Figure, rt_2_months: go.Figure) -> go.Figure:
    def format_viz(fig: go.Figure) -> go.Figure:
        fig.update_layout(height=1000, width=1500)
        fig.update_layout(barmode='stack')
        fig.update_layout(plot_bgcolor='white')
        fig.layout.annotations

        fig.layout.annotations[0].update(x=0.01, xanchor='left')
        fig.layout.annotations[1].update(x=0.01, xanchor='left')
        fig.layout.annotations[2].update(x=0.53, xanchor='left')

        fig['layout'].update(
            xaxis=dict(
                tickformat="%m/%d",
                tickmode='linear',
            )
        )
        fig['layout'].update(
            xaxis2=dict(
                tickformat="%m/%d",
                tickmode='linear',
                tickangle=45,
            )
        )

        fig['layout']['yaxis']['title'] = 'Daily Cases'
        fig['layout']['yaxis2']['title'] = 'Rt'
        fig['layout']['yaxis3']['title'] = 'Rt'

        fig.update_yaxes(showline=True, linewidth=2, linecolor='black')
        fig.update_xaxes(showline=True, linewidth=2, linecolor='black')

        return fig

    def initialize_fig(fig1, fig2, fig3) -> go.Figure:
        combined_fig = make_subplots(
            rows=2, cols=2,
            specs=[[{'colspan': 2}, None], [{}, {}]],
            subplot_titles=(
                "TMC County Cases (Past 2 Weeks)",
                "TMC Rt Estimate (Past 2 Weeks)",
                "TMC Rt Estimate (Past 2 Months)",
            ),
            horizontal_spacing=0.05,
            vertical_spacing=0.1,
        )

        for trace in fig1.data:
            combined_fig.add_trace(trace, row=1, col=1)

        for trace in fig2.data:
            combined_fig.add_trace(trace, row=2, col=1)

        for trace in fig3.data:
            combined_fig.add_trace(trace, row=2, col=2)

        return combined_fig

    combined_fig = initialize_fig(county_cases, rt_2_weeks, rt_2_months)
    final_fig = format_viz(combined_fig)
    return final_fig


def save_figure(fig: go.Figure, path: str) -> None:
    fig.write_image(path, width=1500, height=1000, scale=2)


def main():
    tmc_rt = get_clean_rt()

    tmc_county_cases, tmc_summary_prepped = get_case_dfs()

    rt_2_weeks_viz = rt_estimate_plot(
        filter_df_date(tmc_rt, 14),
    )

    rt_2_months_viz = rt_estimate_plot(
        filter_df_date(tmc_rt, 60),
    )

    county_cases_viz = county_cases_plot(tmc_county_cases, tmc_summary_prepped)
    combined_viz = create_viz(county_cases_viz, rt_2_weeks_viz, rt_2_months_viz)

    # output
    tmc_rt.to_csv('data/tmc/rt.csv', index=False, lineterminator='\r\n')
    save_figure(combined_viz, 'data/tmc/stacked_plot.png')
