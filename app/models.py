from app.services.db_service import mongo_service
from datetime import datetime

crypto_collection = mongo_service.get_collection("crypto_trends")

def insert_crypto_data(data):
    schema = {
        "crypto_name": data.get("crypto_name", "Unknown"),
        "source": data.get("source", "Unknown"),
        "timestamp": data.get("timestamp", datetime.now().isoformat()),
        "mentions": data.get("mentions", 0),
        "likes": data.get("likes", 0),
        "shares": data.get("shares", 0),
        "comments": data.get("comments", 0),
        "sentiment_score": data.get("sentiment_score", 0.0),
        "trend_score": data.get("trend_score", 0.0),
        "raw_data": data.get("raw_data", {})
    }
    return crypto_collection.insert_one(schema)

__all__ = ["crypto_collection", "insert_crypto_data"]
