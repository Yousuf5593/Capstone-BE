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
positive_count = neutral_count = negative_count = 0

def get_sentiment_score(text):
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']


def categorize_sentiment(score):
    global positive_count, neutral_count, negative_count

    if score >= 0.05:
        positive_count += 1
        return "Positive"
    elif score <= -0.05:
        negative_count += 1
        return "Negative"
    else:
        negative_count += 1
        return "Neutral"

def fetch_recent_tweets(keyword, max_tweets=10):
    """
    Fetch recent tweets using Tweepy and store them in the MongoDB database.
    Ensures that fetched tweets are appended to `twitter_data` in the existing document.
    """
    max_tweets = max(10, min(max_tweets, 100))  # ✅ Ensure it's between 10 and 100
    query = f"{keyword} -is:retweet lang:en"

    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_tweets, tweet_fields=["created_at"])

        if tweets.data:
            sentiments = []

            # with open("tweitter_data.json", "w", encoding="utf-8") as f:
            #     json.dump(tweets.data , f, indent=4, ensure_ascii=False)
            for tweet in tweets.data:
                sentiment_score = get_sentiment_score(tweet.text)  # Range: -1 (negative) to +1 (positive)
                sentiments.append(sentiment_score)

                
            avg_sentiment_score = sum(sentiments) / len(sentiments) if sentiments else 0
            sentiment_label = categorize_sentiment(avg_sentiment_score)

            print("sentiment_label: ", sentiment_label)

            response = {
                "crypto": keyword,
                "avg_sentiment_score": round(avg_sentiment_score, 2),
                "final_sentiment": sentiment_label,
                "positive_count": positive_count,
                "neutral_count": neutral_count,
                "negative_count": negative_count,
            }
            return response

            

        else:
            print("No tweets found for the given keyword.")

    except tweepy.errors.TooManyRequests:
        print("❌ Rate Limit Exceeded. Retrying in 15 minutes...")
        time.sleep(900)  # ✅ Wait 15 minutes (900 seconds) before retrying
        return fetch_recent_tweets(keyword, max_tweets)


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