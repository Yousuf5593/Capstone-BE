from collections import defaultdict
import pandas as pd
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import timedelta

from .helper import get_prices_avg
from app.models import crypto_tweet_repo


analyzer = SentimentIntensityAnalyzer()
# Load the dataset
with open("coins_mapping.json", "r", encoding="utf-8") as f:
    crypto_dict = json.load(f)

def detect_crypto(hashtags):
    if not isinstance(hashtags, list):
        return None

    # Loop through the crypto_dict to check for matches
    for coin, keywords in crypto_dict.items():
        if any(keyword.lower() in [hashtag.lower() for hashtag in hashtags] for keyword in keywords):
            return coin
    return None

def get_sentiment_score(text):
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']


def categorize_sentiment(score):

    if score >= 0.05:
        return "Bullish"
    elif score <= -0.05:
        return "Bearish"
    else:
        return "Neutral"

def hybrid_sentiment_classification(row):
    counts = {
        "Bullish": row["positive_count"],
        "Bearish": row["negative_count"],
        "Neutral": row["neutral_count"]
    }

    total = sum(counts.values())
    max_count = max(counts.values())
    majority_sentiment = max(counts, key=counts.get)

    # Use majority only if clearly dominant (> 50%)
    if max_count / total > 0.5:
        return majority_sentiment
    else:
        return categorize_sentiment(row["avg_sentiment_score"])


def process_coins_sentiment_analysis(start_date, end_date):
    
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if start_date == end_date:
        end_date = end_date + timedelta(days=1)

    # Query MongoDB for the relevant date range
    query = {
        "tweet.date": {
            "$gte": start_date,
            "$lte": end_date
        }
    }

    cursor = crypto_tweet_repo.find_tweets(query)

    # Convert cursor to DataFrame
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return []

    # Extract 'date' and 'content' from the 'tweet' dictionary using .apply() with lambda functions
    df["tweet_date"] = df["tweet"].apply(lambda x: x.get("date") if isinstance(x, dict) else None)
    df["tweet_content"] = df["tweet"].apply(lambda x: x.get("content") if isinstance(x, dict) else None)
    df['price_data'] = df['coins'].apply(lambda x: x.get('symbols', {}))

    # Ensure datetime format for 'tweet_date' column
    df["tweet_date"] = pd.to_datetime(df["tweet_date"], errors='coerce')
    

    # Apply crypto detection using hashtags
    df["crypto"] = df["tweet"].apply(lambda x: detect_crypto(x.get("hashtags", [])) if isinstance(x, dict) else None)

    # Filter out rows where no crypto was detected
    df_filtered = df.dropna(subset=["crypto"]).copy()

    avg_market_caps, avg_coins_prices = get_prices_avg(df_filtered)

    df_filtered["sentiment_score"] = df_filtered["tweet_content"].apply(get_sentiment_score)
    df_filtered["sentiment"] = df_filtered["sentiment_score"].apply(categorize_sentiment)
    # Aggregate sentiment by crypto
    sentiment_summary = df_filtered.groupby(["crypto"]).agg(
        avg_sentiment_score=("sentiment_score", "mean"),
        sentiment_count=("sentiment", "count"),
        positive_count=("sentiment", lambda x: (x == "Bullish").sum()),
        negative_count=("sentiment", lambda x: (x == "Bearish").sum()),
        neutral_count=("sentiment", lambda x: (x == "Neutral").sum())
    ).reset_index()

    # Determine final sentiment classification for each crypto
    sentiment_summary["final_sentiment"] = sentiment_summary.apply(hybrid_sentiment_classification, axis=1)
    sentiment_summary["avg_market_cap"] = sentiment_summary["crypto"].apply(lambda x: avg_market_caps.get(x, 0))
    sentiment_summary["avg_coins_prices"] = sentiment_summary["crypto"].apply(lambda x: avg_coins_prices.get(x, 0))
    response = {
        "sentiment_summary": sentiment_summary.to_dict(orient="records"),
    }
    return response


def get_sentiment_summary_for_range(date_range, popularity_filter="sentiment_count"):
        
    date_range = pd.to_datetime(date_range)

    start_date = date_range
    end_date = start_date + timedelta(days=1)
    query = {
        "tweet.date": {
            "$gte": start_date,
            "$lte": end_date
        }
    }

    cursor = crypto_tweet_repo.find_tweets(query)
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return []

    # Extract 'date' and 'content' from the 'tweet' dictionary using .apply() with lambda functions
    df["tweet_date"] = df["tweet"].apply(lambda x: x.get("date") if isinstance(x, dict) else None)
    df["tweet_content"] = df["tweet"].apply(lambda x: x.get("content") if isinstance(x, dict) else None)
    df["crypto"] = df["tweet"].apply(lambda x: detect_crypto(x.get("hashtags", [])) if isinstance(x, dict) else None)
    df["coin_price"] = df["tweet"].apply(lambda x: x.get("price", 0) if isinstance(x, dict) else 0)
    df["market_cap"] = df["tweet"].apply(lambda x: x.get("market_cap", 0) if isinstance(x, dict) else 0)

    df = df.dropna(subset=["crypto"])
    df["sentiment_score"] = df["tweet_content"].apply(get_sentiment_score)
    df["sentiment"] = df["sentiment_score"].apply(categorize_sentiment)

    avg_market_caps, avg_coins_prices = get_prices_avg(df)
    

    grouped = df.groupby("crypto").agg(
        avg_sentiment_score=("sentiment_score", "mean"),
        sentiment_count=("sentiment", "count"),
        positive_count=("sentiment", lambda x: (x == "Bullish").sum()),
        negative_count=("sentiment", lambda x: (x == "Bearish").sum()),
        neutral_count=("sentiment", lambda x: (x == "Neutral").sum())
    ).reset_index()

    if popularity_filter not in grouped.columns:
        popularity_filter = "sentiment_count"
    grouped = grouped.sort_values(by=popularity_filter, ascending=False)

    grouped["final_sentiment"] = grouped["avg_sentiment_score"].apply(categorize_sentiment)
    grouped["avg_market_cap"] = grouped["crypto"].apply(lambda x: avg_market_caps.get(x, 0))
    grouped["avg_coins_prices"] = grouped["crypto"].apply(lambda x: avg_coins_prices.get(x, 0))

    return grouped.to_dict(orient="records")


def process_tweet_data():

    def determine_final_sentiment(positive: int, negative: int, neutral: int) -> str:
        if positive >= negative and positive >= neutral:
            return "Bullish"
        elif negative >= positive and negative >= neutral:
            return "Bearish"
        else:
            return "Neutral"

    with open("tweets.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    sentiment_summary = []

    for coin, tweets in raw_data.items():
        coin_upper = coin.upper()
        scores = []
        counts = {"Bullish": 0, "Bearish": 0, "Neutral": 0}

        for tweet in tweets:
            score = get_sentiment_score(tweet)
            sentiment = categorize_sentiment(score)
            counts[sentiment] += 1
            scores.append(score)
        
        avg_score = sum(scores) / len(scores) if scores else 0
        final_sentiment = determine_final_sentiment(
            counts["Bullish"], counts["Bearish"], counts["Neutral"]
        )

        sentiment_summary.append({
            "crypto": coin_upper,
            "avg_sentiment_score": avg_score,
            "sentiment_count": len(tweets),
            "positive_count": counts["Bullish"],
            "negative_count": counts["Bearish"],
            "neutral_count": counts["Neutral"],
            "final_sentiment": final_sentiment
        })

    return {
        "sentiment_summary": sentiment_summary
    }