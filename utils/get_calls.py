import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json

def get_merged_data():
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
    return merged_df

def get_news_data():
    with sqlite3.connect('data-log.db') as conn:
            cursor = conn.cursor()

            # Fetch the corresponding game data from the database by game_id
            cursor.execute("""
                        SELECT * FROM espn_news 
                        """)
    columns = ['title', 'date', 'link', 'image_url', 'relevant', 'ai_score']
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    return df

def get_team_ratings(start_date, end_date):
    # Connect to SQLite database
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Execute the query to get articles within the date range where relevant is None
        cursor.execute('''
        SELECT * FROM espn_news
        ''')
    # 1
    # Fetch all results
    data = cursor.fetchall()

    columns = ['title', 'date', 'link', 'image_url', 'relevant', 'ai_score']
    df = pd.DataFrame(data, columns=columns)

    df['date'] = df['date'].apply(lambda x: pd.to_datetime(x))
    df = df[(df['date']>=start_date) & (df['date']<=end_date)]

    final_results = []

    df = df[df['ai_score'].notna()]
    for ix, row in df.iterrows():
        ai_score = json.loads(row['ai_score'].strip().replace('\n', ''))
        df_keys = list(ai_score.keys())
        #df.at[ix, 'ai_score'] = 
        if df_keys:
            if 'result' in ai_score or 'results' in ai_score:
                for game in ai_score[df_keys[0]]:
                    final_results.append(game)
            else:
                final_results.append(row['ai_score'])

    def cast_int_or_zero(value):
        try:
            return(int(value))
        except:
            return None
        
    df = pd.DataFrame(final_results)
    df[0] = df[0].apply(lambda x: x.replace('\n', '').strip() if type(x) == str else x)
    # Melt the DataFrame to long format
    df_melted = df.melt(var_name='TEAM')
    # Drop rows with NaN values in the Score column
    df_melted['value'] = df_melted['value'].apply(lambda x: cast_int_or_zero(x))
    df_melted = df_melted.dropna(subset=['value'])

    # Group by TEAM and calculate AVG and SUM
    df_grouped = df_melted.groupby('TEAM')['value'].agg(['mean', 'sum']).reset_index()

    # Rename columns
    df_grouped.columns = ['TEAM', 'AVG', 'SUM']

    teams = ['Arizona Cardinals',
    'Baltimore Ravens',
    'Buffalo Bills',
    'Chicago Bears',
    'Cincinnati Bengals',
    'Dallas Cowboys',
    'Denver Broncos',
    'Detroit Lions',
    'Green Bay Packers',
    'Houston Texans',
    'Indianapolis Colts',
    'Jacksonville Jaguars',
    'Kansas City Chiefs',
    'Los Angeles Chargers',
    'Miami Dolphins',
    'Minnesota Vikings',
    'New Orleans Saints',
    'New York Giants',
    'New York Jets',
    'Philadelphia Eagles',
    'San Francisco 49ers',
    'Seattle Seahawks',
    'Tampa Bay Buccaneers',
    'Washington Commanders',]

    return df_grouped[df_grouped['TEAM'].isin(teams)]


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