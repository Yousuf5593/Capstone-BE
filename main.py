from flask import Flask, request, abort, jsonify
from flask_cors import CORS
from app.routes import api

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

app.register_blueprint(api, url_prefix="/api")

# @app.route('/getSquare', methods=['POST'])
# def get_square():
#     if not request.json or 'number' not in request.json:
#         abort(400)
#     num = request.json['number']

#     return jsonify({'answer': num ** 2})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)