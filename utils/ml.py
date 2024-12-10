import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

from get_calls import *


def get_all_merged_data():
    """gets the bovada betting data and espn expert data"""
    with sqlite3.connect('data-log.db') as conn:
        cursor = conn.cursor()

        # Fetch the corresponding game data from the database by game_id
        cursor.execute("""
                    SELECT * FROM merged_data 
                    """)
    columns = ['game_id', 'Matchup', 'Projected Winner', 'Ranking', 'alt_game_id',
            'Week', 'Game', 'Time', 'pct', 'message', 'IngestTime']
    merged_df = pd.DataFrame(cursor.fetchall(), columns=columns)
    merged_df.drop(columns=['game_id','alt_game_id', 'Game', 'pct'], inplace=True)
    return merged_df


start, end = get_start_end()
bovada_data = get_bovada_data(start, end)
news_data, team_rating = get_transformed_news_data(start, end)
expert_data = get_expert_data(start, end)
merged_data = get_all_merged_data()

bovada_data = bovada_data.sort_values('bets', ascending=False).drop_duplicates(subset='game_id', keep='first')
# Step 1: Merge team_rating with bovada_data on the home_team column
bovada_data = bovada_data.merge(team_rating, left_on='home_team', right_on='Team', how='left')
bovada_data = bovada_data.rename(columns={'AI News Sentiment Score': 'Home AI News Sentiment'})
bovada_data = bovada_data.drop(columns=['Team'])

# Step 2: Merge team_rating with bovada_data on the away_team column
bovada_data = bovada_data.merge(team_rating, left_on='away_team', right_on='Team', how='left')
bovada_data = bovada_data.rename(columns={'AI News Sentiment Score': 'Away AI News Sentiment'})
bovada_data = bovada_data.drop(columns=['Team'])
bovada_data['Matchup'] = bovada_data.apply(lambda x: x['home_team'] + ' vs ' + x['away_team'], axis=1)

data = pd.merge(bovada_data, merged_data, on='Matchup', how='left')
data = data.drop(columns=['points', 'Ranking', 'Time', 'IngestTime', 'game_id', 'Matchup', 'Projected Winner', 'Week']).drop_duplicates(subset='home_team')

# Normalize the data
scaler = MinMaxScaler()

# Normalize win_diff, home_win, away_win, Home AI News Sentiment, Away AI News Sentiment
data[['win_diff', 'home_win', 'away_win']] = scaler.fit_transform(data[['win_diff', 'home_win', 'away_win']].fillna(0))

# Fill NaN values in AI News Sentiment with 0 and normalize
data[['Home AI News Sentiment', 'Away AI News Sentiment']] = scaler.fit_transform(data[['Home AI News Sentiment', 'Away AI News Sentiment']].fillna(0))

# Extract expert opinions percentage from message
data['expert_opinion'] = data['message'].str.extract(r'(\d+)%').astype(float) / 100

# Normalize expert_opinion
data['expert_opinion'] = scaler.fit_transform(data[['expert_opinion']])

# Define weights for each factor
weights = {
    'win_diff': 0.4,
    'home_win': 0.2,
    'away_win': 0.2,
    'Home AI News Sentiment': 0.1,
    'Away AI News Sentiment': 0.1,
    'expert_opinion': 0.3
}

# Calculate scores for home and away teams
data['home_score'] = (
    data['win_diff'] * weights['win_diff'] +
    (1 - data['home_win']) * weights['home_win'] +  # Invert home_win because more negative is better
    data['Home AI News Sentiment'] * weights['Home AI News Sentiment'] +
    data['expert_opinion'] * weights['expert_opinion']
)

data['away_score'] = (
    data['win_diff'] * weights['win_diff'] +
    (1 - data['away_win']) * weights['away_win'] +  # Invert away_win because more negative is better
    data['Away AI News Sentiment'] * weights['Away AI News Sentiment'] +
    (1 - data['expert_opinion']) * weights['expert_opinion']
)

# Calculate confidence score
data['confidence_score'] = data['home_score'] - data['away_score']

data['ranking'] = data['confidence_score'].apply(abs)
def get_pick(row):
    if row['confidence_score']>0:
        return row['home_team']
    else:
        return row['away_team']
data['pick'] = data.apply(get_pick, axis=1)
data.sort_values('ranking', ascending=False)#.drop(columns=['bets', ''])