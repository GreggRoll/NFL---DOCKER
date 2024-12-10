import logging
import pandas as pd
import hashlib
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import sqlite3

pd.set_option('display.max_columns', None)

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

logger = setup_logger(__name__)

def insert_data_to_db(df, conn, table):
    try:
        df.to_sql(table, conn, if_exists='append', index=False)
    except Exception as e:
        logger.exception(f"SQL INSERT Error on TABLE: {table}\n{e}")

def insert_bovada_data(current_df):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Initialize an empty list to collect DataFrames for concatenation
        games_to_insert_list = []

        # Iterate over each game in the current DataFrame
        for index, game in current_df.iterrows():
            game_id = game['game_id']

            # Fetch the corresponding game data from the database by game_id
            cursor.execute("SELECT * FROM bovada_data WHERE game_id = ?", (game_id,))
            db_game = cursor.fetchone()
            columns = ['date', 'time', 'bets', 'home_team', 'away_team', 'home_win', 'away_win', 'win_differential', 'day', 'points', 'game_id']

            if db_game:
                # Convert db_game to DataFrame for comparison
                db_game_df = pd.DataFrame([db_game], columns=columns)

                # Prepare current game data for comparison
                current_game_df = pd.DataFrame([game], columns=columns)
                # current_game_df['datetime'] = datetime.now().isoformat()

                # Compare with current game data
                if not current_game_df.equals(db_game_df):
                    # Add to the list for bulk insertion
                    games_to_insert_list.append(current_game_df)
            else:
                # If there is no entry for this game in the database, prepare for insertion
                # game['datetime'] = datetime.now().isoformat()
                games_to_insert_list.append(pd.DataFrame([game], columns=columns))

        # Concatenate all DataFrames in the list for bulk insertion
        if games_to_insert_list:
            games_to_insert = pd.concat(games_to_insert_list, ignore_index=True)
            insert_data_to_db(games_to_insert, conn, 'bovada_data')
        logger.info("Added bovada data to SQL")


def insert_matchup_data(current_df):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Initialize an empty list to collect DataFrames for concatenation
        games_to_insert_list = []

        # Iterate over each game in the current DataFrame
        for index, game in current_df.iterrows():
            game_id = game['game_id']

            # Fetch the corresponding game data from the database by game_id
            cursor.execute("SELECT * FROM matchup_data WHERE game_id = ?", (game_id,))
            db_game = cursor.fetchone()
            columns = ['time', 'matchup', 'projected_winner', 'ranking', 'game_id']

            if db_game:
                # Convert db_game to DataFrame for comparison
                db_game_df = pd.DataFrame([db_game], columns=columns)

                # Prepare current game data for comparison
                current_game_df = pd.DataFrame([game], columns=columns)
                # current_game_df['datetime'] = datetime.now().isoformat()

                # Compare with current game data
                if not current_game_df.equals(db_game_df):
                    # Add to the list for bulk insertion
                    games_to_insert_list.append(current_game_df)
            else:
                # If there is no entry for this game in the database, prepare for insertion
                # game['datetime'] = datetime.now().isoformat()
                games_to_insert_list.append(pd.DataFrame([game], columns=columns))

        # Concatenate all DataFrames in the list for bulk insertion
        if games_to_insert_list:
            games_to_insert = pd.concat(games_to_insert_list, ignore_index=True)
            insert_data_to_db(games_to_insert, conn, 'matchup_data')
        logger.info("Added matchup data to SQL")

def insert_expert_data(current_df):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Initialize an empty list to collect DataFrames for concatenation
        games_to_insert_list = []

        # Iterate over each game in the current DataFrame
        for index, game in current_df.iterrows():
            game_id = game['game_id']

            # Fetch the corresponding game data from the database by game_id
            cursor.execute("SELECT * FROM matchup_data WHERE game_id = ?", (game_id,))
            db_game = cursor.fetchone()

            if db_game:
                # Convert db_game to DataFrame for comparison
                db_game_df = pd.DataFrame([db_game])

                # Prepare current game data for comparison
                current_game_df = pd.DataFrame([game])
                current_game_df['datetime'] = datetime.now().isoformat()

                # Compare with current game data
                if not current_game_df.equals(db_game_df):
                    # Add to the list for bulk insertion
                    games_to_insert_list.append(current_game_df)
            else:
                # If there is no entry for this game in the database, prepare for insertion
                game['datetime'] = datetime.now().isoformat()
                games_to_insert_list.append(pd.DataFrame([game]))

        # Concatenate all DataFrames in the list for bulk insertion
        if games_to_insert_list:
            games_to_insert = pd.concat(games_to_insert_list, ignore_index=True)
            insert_data_to_db(games_to_insert, conn, 'expert_data')
        logger.info("Added expert data to SQL")

def insert_merge_data(current_df):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Initialize an empty list to collect DataFrames for concatenation
        games_to_insert_list = []

        # Iterate over each game in the current DataFrame
        for index, game in current_df.iterrows():
            game_id = game['game_id']

            # Fetch the corresponding game data from the database by game_id
            cursor.execute("SELECT * FROM merged_data WHERE game_id = ?", (game_id,))
            db_game = cursor.fetchone()

            if db_game:
                # Convert db_game to DataFrame for comparison
                db_game_df = pd.DataFrame([db_game])

                # Prepare current game data for comparison
                current_game_df = pd.DataFrame([game])

                # Compare with current game data
                if not current_game_df.equals(db_game_df):
                    # Add to the list for bulk insertion
                    games_to_insert_list.append(current_game_df)
            else:
                # If there is no entry for this game in the database, prepare for insertion
                games_to_insert_list.append(pd.DataFrame([game]))

        # Concatenate all DataFrames in the list for bulk insertion
        if games_to_insert_list:
            games_to_insert = pd.concat(games_to_insert_list, ignore_index=True)
            insert_data_to_db(games_to_insert, conn, 'merged_data')
        logger.info("Added MERGED data to SQL")

def generate_game_id(row):
    try:
        # Example: Use a combination of date, home team, and away team to generate a unique ID
        identifier = f"{row['date']}_{row['home_team']}_{row['away_team']}"
        return hashlib.md5(identifier.encode()).hexdigest()
    except Exception as e:
        logger.exception("Generate Game error")

def convert_to_int(value):
    try:
        if value == 'EVEN':
            return 0
        if value.startswith('+'):
            return int(value[1:])
        elif value.startswith('-'):
            return int(value)
        else:
            return int(value)
    except Exception as e:
        logger.exception("Convert to int error")
        return -1

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
            print(f"Error!: {e} {item}")

    def get_bets(string):
        try:
            return int(string.split('+')[1].strip())
        except Exception as e:
            print(f"Get Bets Error {e} {string}")
            return None

    df = pd.DataFrame(data)
    # df["Home Spread"] = df.apply(lambda row: concat_values(row[10], row[11]), axis=1)
    # df["Away Spread"] = df.apply(lambda row: concat_values(row[12], row[13]), axis=1)
    # df["total_home"] = df.apply(lambda row: concat_values(row[16], row[17], row[18]), axis=1)
    # df["total_away"] = df.apply(lambda row: concat_values(row[19], row[20], row[21]), axis=1)
    df['bets'] = df[2].apply(lambda x: get_bets(x))
    df.rename(columns = {
        0: "date",
        1: "time",
        6: "home_team",
        7: "away_team",
        14: "home_win",
        15: "away_win"
    }, inplace=True)

    df = df[["date", "time", "bets", "home_team", "away_team", "home_win", "away_win"]]

    def create_differential(home_win, away_win):
        try:
            home = int(home_win)
            away = int(away_win)
            return abs(home-away)
        except:
            return 0

    df["win_differential"] = df.apply(lambda row: create_differential(row['home_win'], row['away_win']), axis=1)



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
    insert_bovada_data(current_df)
    return current_df

def generate_matchups(df):
    # Ensure DateTime is properly formatted
    df['DateTime'] = pd.to_datetime(df['date']+' '+df['time'])

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
            "time": row['DateTime'].strftime('%A %H:%M %p'),
            "projected_winner": projected_winner,
            "ranking": points,
            "alt_game_id": row['game_id']
        })
    matchup_df = pd.DataFrame(matchups_data).sort_values("ranking", ascending=False)
    # insert_matchup_data(matchup_df)
    return matchup_df

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
            "lar": "Rams", "tb": "Tampa Bay", "dal": "Dallas Cowboys", "ind": "Indianappolis Colts", 
            "sea": "Seattle Seahawks"
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
        insert_expert_data(df)
        return df[["game_id", "week", "Game", "Time", "pct", "message"]]
    except Exception as e:
        logger.exception(f"get espn data, {e}")

def insert_betting_expert_data(start_date, end_date):
    logger.info("STARTING data grab")
    bovada_df = get_data(start_date, end_date)
    matchup_df = generate_matchups(bovada_df)
    expert_df = get_espn_expert_data()
    expert_df.sort_values("pct", ascending=False)
    merged_df = pd.merge(matchup_df, expert_df, on="game_id")
    merged_df.drop(columns=["time"], inplace=True)
    merged_df["IngestTime"] = datetime.now().strftime("%m/%d %H:%M")
    # merged_df = merged_df[["IngestTime", "week", "Game", "Time", "projected_winner", "ranking", "message"]]
    merged_df["ranking"] = merged_df["ranking"]+1
    insert_merge_data(merged_df)
    logger.info("COMPLETED data grab")