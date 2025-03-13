from flask import Flask
from flask_cors import CORS
import pymongo
import os
from dotenv import load_dotenv

load_dotenv()


def create_app():
    app = Flask(__name__)
    CORS(app)

    app.config["MONGO_URI"] = os.getenv("MONGO_URI")

    from app.routes import api
    app.register_blueprint(api, url_prefix="/api")

    return app
