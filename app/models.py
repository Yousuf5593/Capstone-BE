import ast
from datetime import datetime
from app.services.db_service import mongo_service
from pymongo.errors import BulkWriteError

class CryptoTweet:
    def __init__(self):
        self.collection = mongo_service.db['crypto_tweets']
        
    def create_tweet(self, data):
        """Main method to create a new tweet document"""
        document = {
            'user': self._parse_user(data),
            'tweet': self._parse_tweet(data),
            'coins': self._parse_coins(data),
            # 'metadata': self._parse_metadata(data),
            'tweet_hash': data.get('tweet_hash'),
            'created_at': datetime.now()
        }
        return self.collection.insert_one(document)

    def create_tweets_bulk(self, data_list):
        """
        Bulk insert method to create multiple tweet documents.
        Skips duplicates based on unique tweet_hash.
        """
        documents = []
        for data in data_list:
            document = {
                'user': self._parse_user(data),
                'tweet': self._parse_tweet(data),
                'coins': self._parse_coins(data),
                'tweet_hash': data.get('tweet_hash'),
                'created_at': datetime.now()
            }
            documents.append(document)

        if documents:
            try:
                result = self.collection.insert_many(documents, ordered=False)
                return result
            except BulkWriteError as bwe:
                # Log number of successful inserts
                inserted = len(bwe.details.get("writeErrors", []))
                print(f"Duplicate entries skipped. Inserted {len(documents) - inserted} documents.")
                return len(documents) - inserted  # or return None / partial result if preferred
        return None
    
    def find_tweets(self, query):
        """Main method to create a new tweet document"""
        return self.collection.find(query)

    def _parse_user(self, data):
        user_data = data.get('user', {})
        return {
            'name': user_data.get('name'),
            'location': user_data.get('location'),
            'followers': float(user_data.get('followers', 0))
        }

    def _parse_tweet(self, data):
        tweet_data = data.get('tweet', {})
        return {
            'content': tweet_data.get('text'),
            'hashtags': self._safe_parse(tweet_data.get('hashtags', [])),
            'date': self._parse_date(tweet_data.get('date'))
        }

    def _parse_coins(self, data):
        coin_data = data.get('coins', {})
        return {
            'symbols': self._safe_parse(coin_data.get('symbols', [])),
            'names': self._safe_parse(coin_data.get('names', [])),
            'price_data': self._parse_price_data(coin_data.get('price_data', {}))
        }

    def _parse_metadata(self, data):
        return {
            'raw_text': data.get('text'),
            'original_data': data  # Store original CSV data for reference
        }

    def _parse_date(self, date_str):
        try:
            # Try full datetime first
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            try:
                # Try fallback by appending time if format is just date
                return datetime.strptime(date_str + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
            except Exception as e2:
                print(f"Error parsing date: {date_str}. Error: {e2}")
                return None

    def _safe_parse(self, value):
        """Safely parse string representations of lists/dicts"""
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                pass
        return value

    def _parse_price_data(self, price_str):
        try:
            return eval(price_str) if isinstance(price_str, str) else price_str
        except Exception as e:
            return {}

# Singleton instance
crypto_tweet_repo = CryptoTweet()