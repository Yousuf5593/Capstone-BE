import time
import os
import pandas as pd
import random
import tweepy

from app.services.db_service import mongo_service

twitter_api_key = os.getenv('TWITTER_API_KEY')
twitter_api_secret_key = os.getenv('TWITTER_API_SECRET_KEY')

twitter_bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
client = tweepy.Client(bearer_token=twitter_bearer_token)

#import the dataset
file_path = "CryptocurrencyData.csv"
df = pd.read_csv(file_path)


capstone_collection = mongo_service.get_collection('crypto_trends')

def fetch_recent_tweets(keyword, max_tweets=10):
    """
    Fetch recent tweets using Tweepy and store them in the MongoDB database.
    Ensures that fetched tweets are appended to `twitter_data` in the existing document.
    """
    max_tweets = max(10, min(max_tweets, 100))  # ✅ Ensure it's between 10 and 100
    query = f"{keyword} -is:retweet lang:en"

    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_tweets)

        if tweets.data:
            formatted_tweets = [
                {
                    "tweet_id": tweet.id,
                    "text": tweet.text,
                    "created_at": tweet.created_at.isoformat(),
                    "user": {
                        "id": tweet.author_id,
                        "username": tweet.author_id,  # Adjust if user details are fetched
                    },
                    "sentiment_score": 0.0,  # Placeholder, update after sentiment analysis
                }
                for tweet in tweets.data
            ]

            existing_document = capstone_collection.find_one({})  # Find existing mock document

            if existing_document:
                # Append new tweets to `twitter_data`
                capstone_collection.update_one(
                    {"_id": existing_document["_id"]},
                    {"$push": {"data.twitter_data": {"$each": formatted_tweets}},
                     "$inc": {"data.mock_tweets": len(formatted_tweets)}}
                )
                return {"message": "Tweets added to existing mock data", "id": str(existing_document["_id"])}
            else:
                # Insert a new document with the fetched tweets
                new_document = {
                    "data": {
                        "is_mock": True,
                        "active_markets": 0,
                        "mock_tweets": len(formatted_tweets),
                        "crypto_data": [],
                        "twitter_data": formatted_tweets
                    }
                }
                result = capstone_collection.insert_one(new_document)
                return {"message": "New document created with tweets", "id": str(result.inserted_id)}

        else:
            print("No tweets found for the given keyword.")

    except tweepy.errors.TooManyRequests:
        print("❌ Rate Limit Exceeded. Retrying in 15 minutes...")
        time.sleep(900)  # ✅ Wait 15 minutes (900 seconds) before retrying
        return fetch_recent_tweets(keyword, max_tweets)

def process_crypto_data():
    processed_data = []
    df.columns = df.columns.str.strip()
    df.replace("-", "0", inplace=True)
    for _, row in df.iterrows():
        try:
            # Convert 24h % change to float safely
            trend_score = float(str(row["24h"]).strip('%')) / 100 if "%" in str(row["24h"]) else 0.0
            
            # Simulate sentiment (-1 to 1)
            sentiment_score = random.uniform(-1, 1)

            processed_data.append({
                "name": row["Coin Name"],
                "symbol": row["Symbol"],
                "price": float(row["Price"].replace(",", "")) if row["Price"] != "0" else 0.0,  # ✅ Convert safely
                # "sentiment_score": round(sentiment_score, 2),
                "trend_score": round(trend_score, 2),
                "market_cap": row["Market Cap"] if row["Market Cap"] != "0" else "Unknown",
                "volume_24h": row["24h Volume"] if row["24h Volume"] != "0" else "Unknown",
                "change_24h": row["24h"] if row["24h"] != "0" else "No Change"
            })
        except ValueError:
            # print(f"Skipping invalid row: {row}")
            continue
        
    return processed_data


def insert_fetched_tweets(tweets):
    """
    Inserts or updates fetched tweets into the twitter_data array in MongoDB.
    """
    if not tweets:
        return {"error": "No tweets to insert"}

    existing_document = capstone_collection.find_one({})  # Get the first document

    # Prepare tweet data for insertion
    formatted_tweets = [
        {
            "tweet_id": tweet.id,
            "text": tweet.text,
            "created_at": tweet.created_at.isoformat(),
            "user": {
                "id": tweet.author_id,
                "username": tweet.author_id,  # Adjust this if fetching user details
            },
            "sentiment_score": 0.0,  # Placeholder, update after sentiment analysis
        }
        for tweet in tweets
    ]

    if existing_document:
        # Append new tweets to existing `twitter_data`
        capstone_collection.update_one(
            {"_id": existing_document["_id"]},
            {"$push": {"data.twitter_data": {"$each": formatted_tweets}}}
        )
        return {"message": "Tweets added to existing mock data", "id": str(existing_document["_id"])}
    else:
        # Insert a new document with the tweets
        new_document = {
            "data": {
                "is_mock": True,
                "active_markets": 0,
                "mock_tweets": len(formatted_tweets),
                "crypto_data": [],
                "twitter_data": formatted_tweets
            }
        }
        result = capstone_collection.insert_one(new_document)
        return {"message": "New document created with tweets", "id": str(result.inserted_id)}