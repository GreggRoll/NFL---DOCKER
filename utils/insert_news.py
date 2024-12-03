import requests
import sqlite3
from datetime import datetime, timedelta
import logging
import json
from bs4 import BeautifulSoup
import pandas as pd
import openai
from dotenv import load_dotenv
import os

# Set your OpenAI API key
load_dotenv()
client = openai.OpenAI(os.getenv('API_KEY'))

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

def insert_news(df):
    conn = sqlite3.connect('../data-log.db')
    cursor = conn.cursor()

    # Function to check if a row with the same title exists and if relevant is not None
    def row_exists_and_relevant_not_none(title):
        cursor.execute("SELECT relevant FROM espn_news WHERE title = ?", (title,))
        row = cursor.fetchone()
        return row is not None and row[0] is not None

    for ix, row in df.iterrows():
        if row_exists_and_relevant_not_none(row['title']):
            # Update the row
            cursor.execute('''
            UPDATE espn_news
            SET date = ?, link = ?, image_url = ?, relevant = ?, ai_score = ?
            WHERE title = ?
            ''', (row['date'], row['link'], row['image_url'], row['relevant'], row['ai_score'], row['title']))
        else:
            # Insert the row if it doesn't exist
            cursor.execute('''
            INSERT OR IGNORE INTO espn_news (title, date, link, image_url, relevant, ai_score)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (row['title'], row['date'], row['link'], row['image_url'], row['relevant'], row['ai_score']))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()

    logger.info("INSERTED NEWS TO espn_news")


def get_espn_news():
    # URL of the page to scrape
    url = "https://www.nfl.com/news/all-news"

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')

        # List to store the scraped data
        scraped_data = []

        # Function to extract article data
        def extract_article_data(article):
            # Extract the title
            title = article.find('h3', class_='d3-o-media-object__title').get_text(strip=True)

            # Extract the date
            date = article.find('p', class_='d3-o-media-object__date').get_text(strip=True)

            # Extract the link to the article
            link = article['href']
            full_link = f"https://www.nfl.com{link}"

            # Extract the image URL
            image_tag = article.find('picture').find('img')
            image_url = image_tag['src'] if image_tag else None

            # Append the extracted data to the list
            scraped_data.append({
                'title': title,
                'date': date,
                'link': full_link,
                'image_url': image_url
            })

        # Find all vertical article containers
        vertical_articles = soup.find_all('div', class_='d3-o-media-object--vertical')
        for article in vertical_articles:
            extract_article_data(article.find('a'))

        # Find all horizontal article containers
        horizontal_articles = soup.find_all('a', class_='d3-o-media-object--horizontal')
        for article in horizontal_articles:
            extract_article_data(article)

        df = pd.DataFrame(scraped_data)
        df['relevant'] = None
        df['ai_score'] = None
        insert_news(df)
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

def get_unclassified(start_date, end_date):
    # Connect to SQLite database
    conn = sqlite3.connect('../data-log.db')
    cursor = conn.cursor()

    # Execute the query to get articles within the date range where relevant is None
    cursor.execute('''
    SELECT * FROM espn_news
    WHERE relevant IS NULL
    ''')
    # 1
    # Fetch all results
    articles = cursor.fetchall()

    # Close the connection
    conn.close()

    columns = ['title', 'date', 'link', 'image_url', 'relevant', 'ai_score']
    df = pd.DataFrame(articles, columns=columns)
    #filter on_days
    df['date'] = df['date'].apply(lambda x: pd.to_datetime(x))
    df = df[(df['date']>=start_date) & (df['date']<=end_date)]
    return df

def update_column(row_name, df):
    # Connect to SQLite database
    conn = sqlite3.connect('../data-log.db')
    cursor = conn.cursor()

    for ix, row in df.iterrows():
        cursor.execute(f'''
            UPDATE espn_news
            SET {row_name} = ?
            WHERE title = ?
            ''', (row[row_name], row['title']))
        
    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()
    logger.info(f"UPDATED {row_name} in espn_news")

def extract_article_text(article_url):
    article_response = requests.get(article_url)
    if article_response.status_code == 200:
        article_soup = BeautifulSoup(article_response.content, 'html.parser')
        article_body = article_soup.find('div', class_='nfl-c-article__body')
        if article_body:
            paragraphs = article_body.find_all('p')
            article_text = "\n".join([p.get_text(strip=True) for p in paragraphs])
            return article_text
    return None

def check_relevance(title):

    completion = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "Reply True or False to whether the following news title is relevant to the odds of a team winning or losing'"},
        {"role": "user", "content": f"{title}"}
    ]
    )

    return completion.choices[0].message.content

def score_article(text):

    completion = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "You are an assistant that reads article texts about sports teams. Your task is to identify the team name mentioned in the article and provide a rating on how the information in the article will affect that team's upcoming game. The rating should be on a scale from 5 to -5, where 5 indicates the team is sure to win and -5 indicates the team is sure to lose."},
        {"role": "system", "content": "Example return: [{'Tampa Bay Buccaneers': 3}]"},
        {"role": "system", "content": "always return json with a list of teams and impacts"},
        {"role": "system", "content": "always reply in the following format: [{'Team': Score}]"},
        {"role": "system", "content": "If data is missing or not relevant return [{'None': Score}]"},
        {"role": "user", "content": f"{text}"}
    ],
     response_format={ "type": "json_object" }
    )

    return completion.choices[0].message.content

def insert_espn_news(start_date, end_date):
    logger.info("Starting espn_news updates")
    get_espn_news()
    df = get_unclassified(start_date, end_date)
    df["relevant"] = df["title"].apply(check_relevance)
    logger.info(f"RAN check_relevance on {len(df)} rows")
    update_column('relevant', df)
    #update_rows(df)
    df = df[df['relevant']=='True']
    df['article_text'] = df['link'].apply(extract_article_text)
    df['ai_score'] = df['article_text'].apply(score_article)
    logger.info(f"RAN score_article on {len(df)} rows")
    update_column('ai_score', df)
    logger.info("Completed espn_news updates")