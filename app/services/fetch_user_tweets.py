import pandas as pd
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import timedelta
from app.models import crypto_tweet_repo

analyzer = SentimentIntensityAnalyzer()

with open("coins_mapping.json", "r", encoding="utf-8") as f:
    crypto_dict = json.load(f)

def detect_crypto_in_text(text):
    detected = set()
    lower_text = text.lower()
    for coin, keywords in crypto_dict.items():
        if any(keyword.lower() in lower_text for keyword in keywords):
            detected.add(coin)
    return list(detected)

def fetch_user_tweets(start_date, end_date, tweets_data):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if start_date == end_date:
        end_date += timedelta(days=1)

    # query DB if tweets_data not passed
    if not tweets_data:
        query = {
            "tweet.date": {"$gte": start_date, "$lte": end_date}
        }
        cursor = crypto_tweet_repo.find_tweets(query)
        df = pd.DataFrame(list(cursor))
    else:
        df = pd.DataFrame(list(tweets_data['related_tweets']))

    if df.empty:
        return {"related_tweets": []}

    # Extract required fields
    df["tweet_date"] = df["tweet"].apply(lambda x: x.get("date") if isinstance(x, dict) else None)
    df["tweet_content"] = df["tweet"].apply(lambda x: x.get("content") if isinstance(x, dict) else None)
    df["user_name"] = df["user"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    df["user_followers"] = df["user"].apply(lambda x: x.get("followers") if isinstance(x, dict) else None)
    df["user_location"] = df["user"].apply(lambda x: x.get("location") if isinstance(x, dict) else None)
    
    # Directly extract the symbols from the coins object
    df["symbols"] = df["coins"].apply(
        lambda x: x.get("symbols", []) if isinstance(x, dict) else []
    )

    # Deduplicate tweets by unique content+user+timestamp
    df["dedup_key"] = df.apply(
        lambda row: f"{row['tweet_content']}-{row['user_name']}-{row['tweet_date']}", axis=1
    )
    df = df.drop_duplicates(subset="dedup_key")

    # Final structure
    final_tweets = df[[
        "tweet_date",
        "tweet_content",
        "user_name",
        "user_followers",
        "user_location",
        "symbols"
    ]].to_dict(orient="records")

    return {"related_tweets": final_tweets}
