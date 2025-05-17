import time
import os
import json
import pandas as pd
import tweepy

from app.services.db_service import mongo_service
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

twitter_api_key = os.getenv('TWITTER_API_KEY')
twitter_api_secret_key = os.getenv('TWITTER_API_SECRET_KEY')

twitter_bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
client = tweepy.Client(bearer_token=twitter_bearer_token)

capstone_collection = mongo_service.get_collection('crypto_trends')

def update_tweet_json_file(tweets, keyword, filename="tweets.json"):
    print("Tweets Data: ", tweets.data)
    # Step 1: Load existing data if the file exists
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    # Step 2: Extract tweet texts from tweets.data
    new_texts = [tweet.text.strip() for tweet in tweets.data]

    # Step 3: Update or create the keyword entry
    if keyword.lower() in data:
        data[keyword.lower()].extend(new_texts)
    else:
        data[keyword.lower()] = new_texts

    # Step 4: Write back to JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def fetch_recent_tweets(keyword, max_tweets=40):
    """
    Fetch recent tweets using Tweepy and store them in the MongoDB database.
    Ensures that fetched tweets are appended to `twitter_data` in the existing document.
    """
    max_tweets = max(10, min(max_tweets, 100))  # ✅ Ensure it's between 10 and 100
    query = f"{keyword} -is:retweet lang:en"

    print(f"Extracting tweets for {keyword} coin")

    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_tweets, tweet_fields=["created_at", "text"])
        if tweets.data:
            update_tweet_json_file(tweets, keyword)

        else:
            print("No tweets found for the given keyword.")
            return []

    except tweepy.errors.TooManyRequests:
        print("❌ Rate Limit Exceeded. Retrying in 15 minutes...")
        time.sleep(900)  # ✅ Wait 15 minutes (900 seconds) before retrying
        return fetch_recent_tweets(keyword, max_tweets)
