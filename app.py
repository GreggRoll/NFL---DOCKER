import logging
# import sqlite3
# import pandas as pd
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
from dash import Dash
import dash_bootstrap_components as dbc
import utils.insert_data as insert_data
import utils.get_calls as get_calls

def setup_logger(name):
    """Set up a logger for a given module."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create file handler which logs even debug messages
    fh = logging.FileHandler('app.log')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    fh.setFormatter(formatter)

    # Add the handler to the logger
    if not logger.handlers:
        logger.addHandler(fh)

    return logger

app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
logger = setup_logger(__name__)

app.layout = html.Div([
    html.H1("Bovada and ESPN Expert Data"),
    dcc.Interval(
        id='table-refresh-component',
        interval=1*60*1000,  # in milliseconds (1 minute)
        n_intervals=0
    ),
    dcc.Interval(
        id='data-refresh-component',
        # interval=12*60*60*1000,  # in milliseconds (12 hrs)
        interval=1*60*1000,  # in milliseconds (1 minute)
        n_intervals=0
    ),
    dbc.Accordion([
        dbc.AccordionItem(
            [
                html.Div(id='data-table')
            ],
            title="Merged Data"
        ),
        dbc.AccordionItem(
            [
                html.Div(id='news-table')
            ],
            title="Ai News Data"
        ),
        dbc.AccordionItem(
            [
                html.Div(id='team-outlook')
            ],
            title="Ai News Team Outlook"
        )
    ])
])

@app.callback([Output('data-table', 'children'),
              Output('news-table', 'children'),
              Output('team-outlook', 'children')],
              [Input('table-refresh-component', 'n_intervals')])
def update_table(n):
"""returns merged, news team rankings and ai news data"""
    start, end = get_calls.get_start_end()
    merged_df = get_calls.get_merged_data()
    team_ratings = get_calls.get_team_ratings(start, end)
    news_df = get_calls.get_news_data()


    return [dash_table.DataTable(
        style_data={
            'whiteSpace': 'normal',
            'height': 'auto',
        },
        data = merged_df.to_dict('records'), 
        columns = [{"name": i, "id": i} for i in merged_df.columns]
        ),
        dash_table.DataTable(
            style_data={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            data = team_ratings.to_dict('records'), 
            columns = [{"name": i, "id": i} for i in team_ratings.columns]
            ),
        dash_table.DataTable(
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                data = news_df.to_dict('records'), 
                columns = [{"name": i, "id": i} for i in news_df.columns]
                )]

@app.callback([Input('data-refresh-component', 'n_intervals')])
def update_data(n):
    start, end = get_calls.get_start_end()
    insert_data.insert_betting_expert_data(start, end)

if __name__ == '__main__':
    app.run_server(debug=True)
    