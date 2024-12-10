import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json

def get_merged_data():
    """gets the bovada betting data and espn expert data"""
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

def get_bovada_data(start, end):
    """gets the bovada betting data and espn expert data"""
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Fetch the corresponding game data from the database by game_id
        cursor.execute("""
                    SELECT * FROM bovada_data 
                    """)
    columns = ['date', 'time', 'bets', 'home_team', 'away_team',
            'home_win', 'away_win', 'win_diff', 'day', 'points', 'game_id']
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date']>=start) & (df['date']<=end)]
    return df

def get_expert_data(start, end):
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Fetch the corresponding game data from the database by game_id
        cursor.execute("""
                    SELECT * FROM expert_data 
                    """)
    columns = ['game', 'time', 'Bell', 'Bowen', 'Clay', 'Fowler', 'Graziano', 'Kahler', 'Martin',
            'Moody', 'Reid', 'Thiry', 'Wicker' ,'week', 'pct', 'message', 'game_id', 'datetime']
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df[(df['datetime']>=start) & (df['datetime']<=end)]
    return df

def get_transformed_news_data(start_date, end_date):
    """Returns the news data and the aggregated team scoring data"""
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
    espn_news_df = pd.DataFrame(data, columns=columns)

    espn_news_df['date'] = espn_news_df['date'].apply(lambda x: pd.to_datetime(x))
    espn_news_df = espn_news_df[(espn_news_df['date']>=start_date) & (espn_news_df['date']<=end_date)]

    df = espn_news_df[espn_news_df['ai_score'].notna()]
    df['ai_score'] = df['ai_score'].str.replace('\n', '')
    items = []
    for ix, row in df.iterrows():
        load = json.loads(row['ai_score'])
        keys =  list(load.keys())
        # if key.lower() in ['result', 'results', 'team', 'team_name']:
        #     return load[key]
        if len(keys)!=0:
            if keys[0].lower() in ['result', 'results', 'team', 'team_name']:
                if len(load[keys[0]]) > 1:
                    try:
                        for r in load[keys[0]]:
                            item = {
                                "title": row['title'],
                                "team": list(r.keys())[0],
                                "ai_rating": list(r.values())[0]
                            }
                            items.append(item)
                    except:
                        item = {
                            'title': row['title'],
                            "team": load[keys[0]],
                            "ai_rating": load[keys[1]]
                            }
                        items.append(item)
                else:
                    item = {
                        "title": row['title'],
                        "team": list(load[keys[0]].keys())[0],
                        "ai_rating": load[keys[0]][list(load[keys[0]].keys())[0]]
                    }
                    items.append(item)
            else:
                item = {
                        "title": row['title'],
                        "team": keys[0],
                        "ai_rating": load[keys[0]]
                    }
                items.append(item)

    df = pd.DataFrame(items)
    teams = ['Arizona Cardinals','Baltimore Ravens','Buffalo Bills','Chicago Bears','Cincinnati Bengals','Dallas Cowboys','Denver Broncos','Detroit Lions','Green Bay Packers','Houston Texans',
    'Indianapolis Colts','Jacksonville Jaguars','Kansas City Chiefs','Los Angeles Chargers','Miami Dolphins','Minnesota Vikings','New Orleans Saints','New York Giants','New York Jets',
    'Philadelphia Eagles','San Francisco 49ers','Seattle Seahawks','Tampa Bay Buccaneers','Washington Commanders']
    df = df[df['team'].isin(teams)]

    merged = pd.merge(df, espn_news_df, on='title', how='left').drop(columns=['date', 'link', 'image_url', 'ai_score', 'relevant'])
    espn_news_df = pd.merge(espn_news_df, merged, on='title', how='left').drop(columns=['ai_score', 'image_url', 'relevant'])
    espn_news_df = espn_news_df[['date', 'title', 'team', 'ai_rating', 'link']]
    espn_news_df.columns = ['Date', 'Article Title', 'Team', 'AI Rating', 'Article Link']
    espn_news_df.dropna(subset= ['Team'], inplace=True)
    merged.drop(columns=['title'], inplace=True)
    team_rating = merged.groupby('team').agg(sum).reset_index()
    team_rating.columns = ['Team', 'AI News Sentiment Score']
    return espn_news_df, team_rating


def get_start_end():
    """get start and end for this weeks games"""
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