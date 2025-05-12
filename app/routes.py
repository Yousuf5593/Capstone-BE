from flask import Blueprint, jsonify, request
from app.services.fetch_user_tweets import fetch_user_tweets
from app.services.db_service import mongo_service
from app.services.fetch_data import fetch_recent_tweets
from app.services.sentimental_analysis import process_coins_sentiment_analysis, get_sentiment_summary_for_range
from app.services.cache_service import get_cache_key, retrieve_from_cache, store_in_cache
from app.services.helper import compare_coin_mentions
from datetime import date

capstone_collection = mongo_service.get_collection('crypto_trends')

api = Blueprint("api", __name__)

# Initialize the app with cache and register the blueprint
def init_app(app, cache):
    api.cache = cache
    app.register_blueprint(api, url_prefix="/api")

@api.route("/fetch/twiiter/data", methods=["GET"])
def fetch_and_store():
    """
    Fetch recent tweets with #Bitcoin (static for now)
    """
    fetch_recent_tweets("Bitcoin", max_tweets=10)
    return jsonify({"message": "Data fetched successfully"}), 200


@api.route('/admin/get_mock_data', methods=['GET'])
def get_mock_data():
    """
    Retrieve the latest mock response data with optional field filtering.
    """
    mock_data = capstone_collection.find_one({}, {"_id": 0})
    
    if not mock_data:
        return jsonify({"message": "No mock data found"}), 404

    # Get requested fields from query params
    requested_fields = request.args.get("fields", "*").split(",")

    # If '*' is provided, return all data
    if "*" in requested_fields:
        return jsonify(mock_data), 200

    # Filter the response to return only requested fields
    filtered_data = {key: mock_data["mock_data"].get(key) for key in requested_fields if key in mock_data["mock_data"]}

    return jsonify(filtered_data), 200

@api.route("/sentiment_analysis", methods=["GET"])
def sentiment_analysis():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    check_cache = request.args.get('check_cache', 'true').lower() == 'true'
    cache_key = get_cache_key(start_date, end_date)


    if check_cache:
        cached_data = retrieve_from_cache(api.cache, cache_key)
        if cached_data:
            print("Returning data from cache.")
            return jsonify(cached_data)
    response = process_coins_sentiment_analysis(start_date, end_date)

    store_in_cache(api.cache, cache_key, response)
    return jsonify(response)

@api.route('/tweets', methods=['POST'])
def get_filtered_tweets():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    check_cache = request.args.get('check_cache', 'true').lower() == 'true'
    cache_key = get_cache_key(start_date, end_date)

    data = request.get_json(force=True)
    coins = data.get("coins")
    tweets_data = []

    print("key: ", cache_key)

    if check_cache:
        cached_data = retrieve_from_cache(api.cache, cache_key)
        if cached_data:
            print("Returning data from cache.")
            tweets_data = cached_data
            print("tweets_data: ", cached_data)
            print("tweets_data: ", type(tweets_data))

    response = fetch_user_tweets(start_date, end_date, coins, tweets_data)
    store_in_cache(api.cache, cache_key, response)
    
    return jsonify(response)


@api.route("/sentiment/compare", methods=["GET"])
def compare_sentiment():

    date_range1 = request.args.get("date_range1")
    date_range2 = request.args.get("date_range2")
    popularity_filter =  request.args.get("popularity_filter", "sentiment_count")
    range1_summary = get_sentiment_summary_for_range(date_range1, popularity_filter)
    range2_summary = get_sentiment_summary_for_range(date_range2, popularity_filter)

    range1_enhanced, range2_enhanced = compare_coin_mentions(range1_summary, range2_summary)

    return {
        "range1": {"sentiment_summary": range1_enhanced},
        "range2": {"sentiment_summary": range2_enhanced}
    }