from flask import Flask
from flask_caching import Cache
from flask_cors import CORS
from app.routes import api

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

# app.register_blueprint(api, url_prefix="/api")
# Initialize the cache (using simple in-memory cache)
app.config['CACHE_TYPE'] = 'simple'  # Using simple in-memory cache
app.config['CACHE_DEFAULT_TIMEOUT'] = 300  # Set cache timeout to 5 minutes (300 seconds)
cache = Cache(app)

from app import routes
routes.init_app(app, cache)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)