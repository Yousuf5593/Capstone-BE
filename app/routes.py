from flask import Blueprint, jsonify, request

from app.services.db_service import mongo_service
from app.services.fetch_data import fetch_recent_tweets
from app.services.sentimental_analysis import process_sentiment_analysis
from app.services.cache_service import get_cache_key, retrieve_from_cache, store_in_cache

capstone_collection = mongo_service.get_collection('crypto_trends')

api = Blueprint("api", __name__)

# Initialize the app with cache and register the blueprint
def init_app(app, cache):
    api.cache = cache
    app.register_blueprint(api, url_prefix="/api")

@api.route("/fetch", methods=["GET"])
def fetch_and_store():
    """
    Fetch recent tweets with #Bitcoin (static for now)
    """
    fetch_recent_tweets("#Bitcoin", max_tweets=10)
    return jsonify({"message": "Data fetched successfully"}), 200

# @api.route("/cryptos", methods=["GET"])
# def get_cryptos():
#     data = list(capstone_collection.find({}, {"_id": 0}))
#     return jsonify(data), 200


@api.route('/admin/update_mock_data', methods=['POST'])

def update_mock_data():
    """
    API to update the mock response data in MongoDB through UI.
    If a document exists, update it. Otherwise, insert a new document with an auto-generated ObjectId.
    """
    data = request.json
    if not data:
        return jsonify({"error": "No data received"}), 400
    
    existing_document = capstone_collection.find_one({})  # Find any existing document

    if existing_document:
        # If document exists, update it
        capstone_collection.update_one(
            {"_id": existing_document["_id"]},
            {"$set": {"data": data}}
        )
        return {"message": "Mock data updated successfully", "id": str(existing_document["_id"])}
    else:
        # If no document exists, insert a new one with auto-generated ObjectId
        new_document = {"data": data}
        result = capstone_collection.insert_one(new_document)
        return {"message": "New mock data inserted", "id": str(result.inserted_id)}


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
    check_cache = request.args.get('check_cache', 'false').lower() == 'true'
    cache_key = get_cache_key(start_date, end_date)

    # if check_cache:
    cached_data = retrieve_from_cache(api.cache, cache_key)
    if cached_data:
        print("Returning data from cache.")
        return jsonify(cached_data)
    response = process_sentiment_analysis(start_date, end_date)

    store_in_cache(api.cache, cache_key, response)
    return jsonify(response)
