import logging
import sqlite3
import pandas as pd
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
from dash import Dash
import dash_bootstrap_components as dbc
import get_data

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
        interval=12*60*60*1000,  # in milliseconds (12 hrs)
        n_intervals=0
    ),
    html.Div(id='data-table')
])

@app.callback(Output('data-table', 'children'),
              [Input('table-refresh-component', 'n_intervals')])
def update_table(n):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Fetch the corresponding game data from the database by game_id
        cursor.execute("""
                    SELECT * FROM merged_data 
                    WHERE IngestTime = (SELECT MAX(IngestTime) FROM merged_data)
                    """)
    columns = ['game_id', 'Matchup', 'Projected Winner', 'Ranking', 'alt_game_id',
               'Week', 'Game', 'Time', 'pct', 'message', 'IngestTime']
    merged_df = pd.DataFrame(cursor.fetchall(), columns=columns)
    merged_df.drop(columns=['game_id','alt_game_id', 'Game', 'pct'], inplace=True)
    return dash_table.DataTable(
        style_data={
            'whiteSpace': 'normal',
            'height': 'auto',
        },
        data = merged_df.to_dict('records'), 
        columns = [{"name": i, "id": i} for i in merged_df.columns]
        )

@app.callback([Input('data-refresh-component', 'n_intervals')])
def update_data(n):
    get_data.main()

if __name__ == '__main__':
    app.run_server(debug=True)