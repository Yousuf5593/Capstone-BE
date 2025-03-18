import pandas as pd
import random
from textblob import TextBlob


# Load the dataset
file_path = "crypto_dataset.csv"
df = pd.read_csv(file_path)

# Convert date to datetime format
df["date"] = pd.to_datetime(df["date"], errors='coerce')

# Define crypto keywords dictionary
crypto_dict = {
    "Ethereum": ["ethereum", "eth", "#ethereum", "#eth"],
    "Solana": ["solana", "sol", "#solana", "#sol"],
    "Polkadot": ["polkadot", "dot", "#polkadot", "#dot"],
    "Dogecoin": ["dogecoin", "doge", "#dogecoin", "#doge"],
    "Litecoin": ["litecoin", "ltc", "#litecoin", "#ltc"],
    "Ripple": ["ripple", "xrp", "#ripple", "#xrp"],
    "Cardano": ["cardano", "ada", "#cardano", "#ada"],
    "Binance Coin": ["binancecoin", "bnb", "#binancecoin", "#bnb"],
    "Chainlink": ["chainlink", "link", "#chainlink", "#link"],
    "Shiba Inu": ["shiba", "#shiba"],
    "VeChain": ["vechain", "#vechain"],
    "Tezos": ["tezos", "#tezos"],
    "Stellar": ["stellar", "#stellar"],
    "Avalanche": ["avalanche", "#avalanche"],
    "Polygon": ["polygon", "#polygon"],
    "NFT": ["nft", "#nft"],
    "Bitcoin": ["bitcoin", "btc", "#bitcoin", "#btc"]
}
# Function to detect crypto mentions using hashtags
def detect_crypto(hashtags):
    # Check if hashtags is a string that looks like a list, if so, use eval to convert it
    if isinstance(hashtags, str):
        try:
            hashtags = eval(hashtags)  # Convert string to list if it's stored as a string
        except Exception as e:
            print(f"Error evaluating hashtags: {e}")
            hashtags = []

    # Ensure hashtags is a list
    if not isinstance(hashtags, list):
        hashtags = []

    # Now loop through the hashtags and match with crypto_dict
    for coin, keywords in crypto_dict.items():
        if any(keyword.lower() in [hashtag.lower() for hashtag in hashtags] for keyword in keywords):
            return coin
    return None

# Perform Sentiment Analysis
def get_sentiment_score(text):
    return TextBlob(str(text)).sentiment.polarity

def categorize_sentiment(score):
    if score > 0.1:
        return "Positive"
    elif score < -0.1:
        return "Negative"
    else:
        return "Neutral"

def determine_final_sentiment(avg_score):
    if avg_score > 0.1:
        return "Positive"
    elif avg_score < -0.1:
        return "Negative"
    else:
        return "Neutral"

def process_sentiment_analysis(start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    df_filtered = df.copy()
    if start_date and end_date:
        df_filtered = df[(df["date"] >= start_date) & 
                 (df["date"] <= end_date)]
    elif start_date:
        df_filtered = df[(df["date"] >= start_date)]
    elif end_date:
        df_filtered = df[(df["date"] >= end_date)]
    
    print(df["date"].min(), df["date"].max())  # Check available date range
    print(df[df["date"] == "2021-02-10"])  # Check if data exists for this date
    # Apply crypto detection
    df_filtered["crypto"] = df_filtered["hashtags"].apply(detect_crypto)
    df_filtered = df_filtered.dropna(subset=["crypto"]).copy()
    
    # Sentiment analysis
    df_filtered["sentiment_score"] = df_filtered["text"].apply(get_sentiment_score)
    df_filtered["sentiment"] = df_filtered["sentiment_score"].apply(categorize_sentiment)
    
    # Aggregate sentiment collectively by crypto
    sentiment_summary = df_filtered.groupby(["crypto"]).agg(
        avg_sentiment_score=("sentiment_score", "mean"),
        sentiment_count=("sentiment", "count"),
        positive_count=("sentiment", lambda x: (x == "Positive").sum()),
        negative_count=("sentiment", lambda x: (x == "Negative").sum()),
        neutral_count=("sentiment", lambda x: (x == "Neutral").sum())
    ).reset_index()
    
    # Determine final sentiment classification
    sentiment_summary["final_sentiment"] = sentiment_summary["avg_sentiment_score"].apply(determine_final_sentiment)
    
    # Select random tweets with user details as related tweets
    related_tweets = df_filtered[["date", "crypto", "text", "user_name", "user_followers", "user_friends", "user_favourites", "user_verified"]]
    related_tweets_sample = related_tweets.sample(n=min(13, len(related_tweets)), random_state=42).to_dict(orient="records")
    
    # Convert to JSON response
    response = {
        "sentiment_summary": sentiment_summary.to_dict(orient="records"),
        "related_tweets": related_tweets_sample
    }
    return response