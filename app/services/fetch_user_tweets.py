# import pandas as pd
# import json
# import random
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# from datetime import timedelta
# from app.models import crypto_tweet_repo
# from collections import defaultdict

# analyzer = SentimentIntensityAnalyzer()
# # Load the dataset

# # df = pd.read_csv(file_path)
# # # Convert date to datetime format
# # df["date"] = pd.to_datetime(df["date"], errors='coerce')

# # Define crypto keywords dictionary
# # crypto_dict = {
# #     "Ethereum": ["ethereum", "eth", "#ethereum", "#eth"],
# #     "Solana": ["solana", "sol", "#solana", "#sol"],
# #     "Polkadot": ["polkadot", "dot", "#polkadot", "#dot"],
# #     "Dogecoin": ["dogecoin", "doge", "#dogecoin", "#doge"],
# #     "Litecoin": ["litecoin", "ltc", "#litecoin", "#ltc"],
# #     "Ripple": ["ripple", "xrp", "#ripple", "#xrp"],
# #     "Cardano": ["cardano", "ada", "#cardano", "#ada"],
# #     "Binance Coin": ["binancecoin", "bnb", "#binancecoin", "#bnb"],
# #     "Chainlink": ["chainlink", "link", "#chainlink", "#link"],
# #     "Shiba Inu": ["shiba", "#shiba"],
# #     "VeChain": ["vechain", "#vechain"],
# #     "Tezos": ["tezos", "#tezos"],
# #     "Stellar": ["stellar", "#stellar"],
# #     "Avalanche": ["avalanche", "#avalanche"],
# #     "Polygon": ["polygon", "#polygon"],
# #     "NFT": ["nft", "#nft"],
# #     "Bitcoin": ["bitcoin", "btc", "#bitcoin", "#btc"]
# # }


# with open("coins_mapping.json", "r", encoding="utf-8") as f:
#     crypto_dict = json.load(f)

# def detect_crypto(hashtags):
#     if not isinstance(hashtags, list):
#         return None

#     # Loop through the crypto_dict to check for matches
#     for coin, keywords in crypto_dict.items():
#         if any(keyword.lower() in [hashtag.lower() for hashtag in hashtags] for keyword in keywords):
#             return coin
#     return None

# def get_sentiment_score(text):
#     sentiment = analyzer.polarity_scores(text)
#     score = sentiment['compound']
    
#     if score > 0.1:
#         return 1  # Positive sentiment
#     elif score < -0.1:
#         return -1  # Negative sentiment
#     else:
#         return 0  # Neutral sentiment


# def categorize_sentiment(score):

#     if score >= 0.05:
#         return "Positive"
#     elif score <= -0.05:
#         return "Negative"
#     else:
#         return "Neutral"


# def fetch_user_tweets(start_date, end_date, coin):

#     start_date = pd.to_datetime(start_date)
#     end_date = pd.to_datetime(end_date)

#     if start_date == end_date:
#         end_date = end_date + timedelta(days=1)

#     # Query MongoDB for the relevant date range
#     query = {
#         "tweet.date": {"$gte": start_date, "$lte": end_date},
#         "coins.symbols": coin.lower()
#     }

#     cursor = crypto_tweet_repo.find_tweets(query)
#     df = pd.DataFrame(list(cursor))

#     if df.empty:
#         return []


#     # Extract tweet and user fields
#     df["tweet_date"] = df["tweet"].apply(lambda x: x.get("date") if isinstance(x, dict) else None)
#     df["tweet_content"] = df["tweet"].apply(lambda x: x.get("content") if isinstance(x, dict) else None)
#     df["crypto"] = coin.upper()
#     df["user_name"] = df["user"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
#     df["user_followers"] = df["user"].apply(lambda x: x.get("followers") if isinstance(x, dict) else None)
#     df["user_location"] = df["user"].apply(lambda x: x.get("location") if isinstance(x, dict) else None)
#     df["price"] = df["coins"].apply(
#         lambda coin_data: coin_data.get("price_data", {}).get(coin, {}).get("price") if isinstance(coin_data, dict) else None
#     )

#     related_tweets = df[["tweet_date", "crypto", "tweet_content", "user_name", "user_followers", "user_location", "price"]]
#     related_tweets_full = related_tweets.to_dict(orient="records")

#     # Extract price data
#     # price_data = {}
#     # for row in df["coins"]:
#     #     if isinstance(row, dict):
#     #         price_data = row.get("price_data", {}).get(coin.lower(), {})
#     #         if price_data:
#     #             break
#     response = {
#         "related_tweets": related_tweets_full,
#         # "price_data": {coin.upper(): price_data}
#     }   


#     return response


import pandas as pd
import json
import random
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import timedelta
from app.models import crypto_tweet_repo
from collections import defaultdict

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
