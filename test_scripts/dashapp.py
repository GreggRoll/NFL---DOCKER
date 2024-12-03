import logging
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
import hashlib
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import random
import sqlite3
from datetime import datetime, timedelta
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

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

def concat_values(x, y, z=None):
    if z:
        return f"{x} {y} {z}"
    return f"{x} {y}"

def get_data(start_date, end_date):
    # Configure ChromeOptions for headless browsing
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")  # This line can be important in certain environments
    options.set_capability('goog:loggingPrefs', {'browser': 'SEVERE'})
    # Initialize the Chrome WebDriver with the specified options
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.bovada.lv/sports/football/nfl")
    # wait for the page to load
    time.sleep(10)
    driver.implicitly_wait(10)
    # get the HTML source
    html = driver.page_source
    # create a BeautifulSoup object
    soup = BeautifulSoup(html, "html.parser")
    # close the driver
    driver.quit()

    data = []
    sections = soup.find_all("section", {"class":"coupon-content more-info"})#soup.find_all("section", {"class":"coupon-content more-info"})
    for game in sections:
        try:
            item = str(game).split('>')
            info = [x.split('<')[0].strip() for x in item if not x.startswith("<")]
            data.append(info)
        except Exception as e:
            pass

    df = pd.DataFrame(data)
    df["Home Spread"] = df.apply(lambda row: concat_values(row[10], row[11]), axis=1)
    df["Away Spread"] = df.apply(lambda row: concat_values(row[12], row[13]), axis=1)
    df["total_home"] = df.apply(lambda row: concat_values(row[16], row[17], row[18]), axis=1)
    df["total_away"] = df.apply(lambda row: concat_values(row[19], row[20], row[21]), axis=1)

    #drop columns
    df.drop(columns = [3, 4, 5, 8, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26], inplace=True)


    columns = ["date", "time", "bets", "home_team", "away_team", "home_win", "away_win", "home_spread", "away_spread", "total_over", "total_under"]
    df.columns = columns

    #remove plus from bets
    df['bets'] = df['bets'].apply(lambda x: x[2:])

    #date operations
    #filter data for date
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')  # Adjust the format if needed
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')  # Adjust the format if needed
        # Ensure the 'date' column in df is of type datetime.date


    # Ensure the 'date' column in df is of type datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    one_week_ago = pd.Timestamp(datetime.today().date() - timedelta(weeks=1))
    df['date'].fillna(one_week_ago, inplace=True)

    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    #create day of the week column
    df["day"] = df['date'].dt.strftime('%A')
    #set back to string
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.reset_index(inplace=True, drop=True)

    # Applying the conversion to the 'win_home' and "Away Win" columns
    df['home_win'] = df['home_win'].apply(convert_to_int)
    df["away_win"] = df["away_win"].apply(convert_to_int)
    #ranking
    home = df[["home_team", 'home_win']].rename(columns={'home_team': 'team', 'home_win': 'odds'})
    away = df[['away_team', "away_win"]].rename(columns={'away_team': 'team', "away_win": 'odds'})
    combined = pd.concat([home, away]).sort_values('odds', ascending=False)
    combined['index'] = combined.index
    combined.index = range(0, 2*len(combined), 2)
    df['points'] = None
    # Iterating over the combined DataFrame to assign ranks
    for i, x in combined.iterrows():
        df.at[x['index'], 'points'] = (i-len(combined))/2+1
    current_df = df.sort_values('points', ascending=False)
    #add game id
    current_df["game_id"] = current_df.apply(generate_game_id, axis=1)
    #change column order
    current_df = current_df[['date', 'day', 'time', 'bets', 'home_team', 'away_team', 'points', 'home_win', 'away_win', 'home_spread', 'away_spread', 'total_over', 'total_under', 'game_id']]
    log_data = current_df[['game_id', 'date', 'home_team', 'away_team', 'home_win', 'away_win', 'points']]
    log_data_if_changed(log_data)
    return current_df

def generate_matchups(df):
    # Ensure DateTime is properly formatted
    df['DateTime'] = pd.to_datetime(df['date'])

    # Sort the DataFrame by DateTime to get matchups from soonest to latest
    sorted_df = df.sort_values(by='DateTime')

    # Prepare data for the DataTable
    matchups_data = []

    team_conversion_dict = {
    "Houston Texans": "HOU",
    "New York Jets": "NYJ",
    "Denver Broncos": "DEN",
    "Baltimore Ravens": "BAL",
    "Jacksonville Jaguars": "JAX",
    "Philadelphia Eagles": "PHI",
    "New Orleans Saints": "NO",
    "Carolina Panthers": "CAR",
    "Las Vegas Raiders": "LV",
    "Cincinnati Bengals": "CIN",
    "Miami Dolphins": "MIA",
    "Buffalo Bills": "BUF",
    "Indianapolis Colts": "IND",
    "Minnesota Vikings": "MIN",
    "Washington Commanders": "WSH",
    "New York Giants": "NYG",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "New England Patriots": "NE",
    "Tennessee Titans": "TEN",
    "Dallas Cowboys": "DAL",
    "Atlanta Falcons": "ATL",
    "Chicago Bears": "CHI",
    "Arizona Cardinals": "ARI",
    "Los Angeles Chargers": "LAC",
    "Cleveland Browns": "CLE",
    "Los Angeles Rams": "LAR",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB",
    "Kansas City Chiefs": "KC",
    'San Francisco 49ers': "SF",
    'Pittsburgh Steelers': "PIT"
    }

    for _, row in sorted_df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        points = row['points']

        # Determine the favored team
        projected_winner = home_team if row['home_win'] < row['away_win'] else away_team

        # Add row data
        matchups_data.append({
            "game_id": f"{team_conversion_dict[home_team]}{team_conversion_dict[away_team]}",
            "matchup": f"{home_team} vs {away_team}",
            "time": row['DateTime'].strftime('%H:%M %p'),
            "projected_winner": projected_winner,
            "ranking": points
        })

    return matchups_data

def get_espn_expert_data():
    # Function to transform the game string
    def transform_game(game):
        try:
            teams = game.split(' at ')
            return teams[0] + teams[1]
        except:
            teams = game.split(' VS ')
            return teams[0] + teams[1]
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")  # This line can be important in certain environments
        options.set_capability('goog:loggingPrefs', {'browser': 'SEVERE'})
        # Initialize the Chrome WebDriver with the specified options
        driver = webdriver.Chrome(options=options)
        driver.get("https://www.espn.com/nfl/picks")
        #time.sleep(10)
        driver.implicitly_wait(10)
        # get the HTML source
        html = driver.page_source
        # create a BeautifulSoup object
        soup = BeautifulSoup(html, "html.parser")
        # close the driver
        driver.quit()

        week = soup.find('h1', class_='headline headline__h1 dib').get_text(strip=True).split('- ')[1]

        # Extract game details
        games = []
        game_rows = soup.select('.Table--fixed-left .Table__TBODY .Table__TR')
        for row in game_rows:
            game_info_element = row.select_one('.wrap-competition a')
            game_time_element = row.select_one('.competition-dates')
            if game_info_element and game_time_element:
                game_info = game_info_element.text
                game_time = game_time_element.text
                games.append((game_info, game_time))

        # Extract expert names
        experts = []
        expert_headers = soup.select('.Table__Scroller .Table__THEAD .Table__TH')
        for header in expert_headers:
            expert_name_element = header.select_one('div')
            if expert_name_element:
                expert_name = expert_name_element.text.strip()
                experts.append(expert_name)

        # Extract picks
        picks = []
        pick_rows = soup.select('.Table__Scroller .Table__TBODY .Table__TR')
        for row in pick_rows:
            pick_row = []
            pick_cells = row.select('.Table__TD')
            for cell in pick_cells:
                team_logo = cell.select_one('img')
                if team_logo:
                    # Extract the team abbreviation from the image URL
                    team = team_logo['src'].split('/')[-1].split('.')[0]
                else:
                    team = None
                pick_row.append(team)
            picks.append(pick_row)

        # Create DataFrame
        data = {'Game': [game[0] for game in games], 'Time': [game[1] for game in games]}
        for i, expert in enumerate(experts):
            data[expert] = [pick[i] for pick in picks]

        data['Game'].append(None)
        data['Time'].append(None)

        df = pd.DataFrame(data)
        df.dropna(subset=["Game"], inplace=True)

        df['week'] = week

        convert_dict = {
            "min": "Vikings", "phi": "Eagles", "bal": "Ravens", "det": "Lions", "mia": "Dolphins",
            "nyj": "Jets", "atl": "Falcons", "gb": "Packers", "hou" : "Texans", "lac": "Chargers",
            "buf": "Bills", "den": "Broncos", "kc": "Chiefs", "chi": "Bears", "sf": "49ers", "pit": "Steelers",
            "no": "Saints", "cin": "Bengals", "ne": "Patriots", "wsh": "Commanders", "ari": "Cardinals", 
            "lar": "Rams"
        }

        for ix, row in df.iterrows():
            values = row.to_list()[2:]
            values = [value for value in values if value is not None]
            values_len = len(values)
            values_dict = {}
            for value in values:
                if value not in values_dict.keys():
                    values_dict[value] = 1
                else:
                    values_dict[value] += 1
            #sorting
            values_dict = dict(sorted(values_dict.items(), key=lambda item: item[1], reverse=True))
            top_key = next(iter(values_dict))
            if top_key in convert_dict:
                converted_key = convert_dict[top_key]
            else:
                converted_key = top_key
            pct = int(values_dict[top_key]/values_len*100)
            message = f"{pct}% of experts chose {converted_key}"
            df.loc[ix, "pct"] = pct
            df.loc[ix, "message"] = message

        df["game_id"] = df["Game"].apply(transform_game)
        return df[["game_id", "week", "Game", "Time", "pct", "message"]]
    except Exception as e:
        logger.exception(f"get espn data, {e}")

def get_start_end():
    today = datetime.now()
    weekday = today.weekday()  # Monday is 0 and Sunday is 6

    # Calculate the start date (Tuesday)
    if weekday >= 1:  # If today is Tuesday or after
        start_date = today - timedelta(days=(weekday - 1))
    else:  # If today is before Tuesday
        start_date = today - timedelta(days=(weekday + 6))

    # Calculate the end date (Monday)
    if weekday <= 0:  # If today is Monday
        end_date = today
    else:  # If today is after Monday
        end_date = today + timedelta(days=(7 - weekday))
    return start_date, end_date

app = dash.Dash(__name__)
logger = setup_logger(__name__)

app.layout = html.Div([
    html.H1("Bovada and ESPN Expert Data"),
    dcc.Interval(
        id='interval-component',
        interval=1*60*1000,  # in milliseconds (1 minute)
        n_intervals=0
    ),
    html.Div(id='data-table')
])

@app.callback(Output('data-table', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_table(n):
    # Your script logic here
    start, end = get_start_end()
    bovada_df = get_data(start, end)
    matchup_df = pd.DataFrame(generate_matchups(bovada_df)).sort_values("ranking", ascending=False)
    expert_df = get_espn_expert_data()
    expert_df.sort_values("pct", ascending=False)
    merged_df = pd.merge(matchup_df, expert_df, on="game_id")
    merged_df.drop(columns=["game_id", "time", "pct", "matchup"], inplace=True)
    merged_df["IngestTime"] = datetime.now().strftime("%m/%d %H:%M")
    merged_df = merged_df[["IngestTime", "week", "Game", "Time", "projected_winner", "ranking", "message"]]
    merged_df["ranking"] = merged_df["ranking"]+1

    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in merged_df.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(merged_df.iloc[i][col]) for col in merged_df.columns
            ]) for i in range(len(merged_df))
        ])
    ])

if __name__ == '__main__':
    app.run_server(debug=True)