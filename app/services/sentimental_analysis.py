import pandas as pd
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import timedelta
from app.models import crypto_tweet_repo
from collections import defaultdict


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
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"


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

    market_caps = defaultdict(list)
    for _, row in df_filtered.iterrows():
        detected_coin = row["crypto"]
        coins_obj = row.get("coins", {})
        price_data = row.get("coins", {}).get("price_data", {})
        if isinstance(price_data, dict):
            symbols = coins_obj.get("symbols", [])
            for symbol in symbols:
                coin_price_data = price_data.get(symbol, {})
                if isinstance(coin_price_data, dict):
                    market_cap = coin_price_data.get("market_cap")
                    if isinstance(market_cap, (int, float)):
                        market_caps[detected_coin].append(market_cap)
    avg_market_caps = {
            coin: (sum(values) / len(values)) if values!=0 else 0
            for coin, values in market_caps.items()
        }

    df_filtered["sentiment_score"] = df_filtered["tweet_content"].apply(get_sentiment_score)
    df_filtered["sentiment"] = df_filtered["sentiment_score"].apply(categorize_sentiment)
    # Aggregate sentiment by crypto
    sentiment_summary = df_filtered.groupby(["crypto"]).agg(
        avg_sentiment_score=("sentiment_score", "mean"),
        sentiment_count=("sentiment", "count"),
        positive_count=("sentiment", lambda x: (x == "Positive").sum()),
        negative_count=("sentiment", lambda x: (x == "Negative").sum()),
        neutral_count=("sentiment", lambda x: (x == "Neutral").sum())
    ).reset_index()

    # Determine final sentiment classification for each crypto
    sentiment_summary["final_sentiment"] = sentiment_summary["avg_sentiment_score"].apply(categorize_sentiment)
    sentiment_summary["avg_market_cap"] = sentiment_summary["crypto"].apply(lambda x: avg_market_caps.get(x, 0))

    # Extract 'name', 'followers', and 'location' from the 'user' dictionary
    # df_filtered["user_name"] = df["user"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
    # df_filtered["user_followers"] = df["user"].apply(lambda x: x.get("followers") if isinstance(x, dict) else None)
    # df_filtered["user_location"] = df["user"].apply(lambda x: x.get("location") if isinstance(x, dict) else None)

    # # Sample random tweets related to the crypto mentions
    # related_tweets = df_filtered[["tweet_date", "crypto", "tweet_content", "user_name", "user_followers", "user_location"]]
    # related_tweets_sample = related_tweets.sample(n=min(13, len(related_tweets)), random_state=42).to_dict(orient="records")
    # related_tweets_full = related_tweets.to_dict(orient="records")
    
    # Prepare the response in the required format
    response = {
        "sentiment_summary": sentiment_summary.to_dict(orient="records"),
        # "related_tweets": related_tweets_full
    }
    return response


def get_sentiment_summary_for_range(date_range, popularity_filter="sentiment_count"):
        
    date_range = pd.to_datetime(date_range)

    start_date = date_range
    end_date = start_date + timedelta(days=1)
    print("date:: ", start_date, end_date)
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
    df["market_cap"] = df["tweet"].apply(lambda x: x.get("market_cap", 0) if isinstance(x, dict) else 0)

    df = df.dropna(subset=["crypto"])
    df["sentiment_score"] = df["tweet_content"].apply(get_sentiment_score)
    df["sentiment"] = df["sentiment_score"].apply(categorize_sentiment)

    grouped = df.groupby("crypto").agg(
        avg_sentiment_score=("sentiment_score", "mean"),
        sentiment_count=("sentiment", "count"),
        positive_count=("sentiment", lambda x: (x == "Positive").sum()),
        negative_count=("sentiment", lambda x: (x == "Negative").sum()),
        neutral_count=("sentiment", lambda x: (x == "Neutral").sum()),
        avg_market_cap=("market_cap", "mean")
    ).reset_index()

    if popularity_filter not in grouped.columns:
        popularity_filter = "sentiment_count"
    grouped = grouped.sort_values(by=popularity_filter, ascending=False)

    grouped["final_sentiment"] = grouped["avg_sentiment_score"].apply(categorize_sentiment)

    return grouped.to_dict(orient="records")
