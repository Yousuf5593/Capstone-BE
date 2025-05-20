# Flask Project Setup Guide

## Prerequisites
Ensure you have the following installed on your system:
- Python (>=3.8)
- pip (Python package manager)
- Virtual Environment (venv)
- MongoDB (if using a local database)

## Setup Instructions

### 1. Clone the Repository
```sh
git clone <your-repo-url>
cd <your-project-folder>
```

### 2. Create and Activate Virtual Environment
#### On Windows (PowerShell)
```sh
python -m venv venv
venv\Scripts\activate
```

#### On macOS/Linux
```sh
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```sh
pip install -r requirements.txt
```

### 4. Setup Environment Variables
Create a `.env` file in the root directory and add the following (modify as needed):
```sh
FLASK_APP=app/main.py
FLASK_ENV=development
MONGO_URI=mongodb://localhost:27017/your_database_name
TWITTER_API_KEY=your_twitter_api_key
TWITTER_API_SECRET_KEY=your_twitter_api_secret
```

### 5. Initialize Database
If using MongoDB, ensure it's running and initialized:
```sh
mongod --dbpath /data/db
```

### 6. Run the Flask Server
```sh
flask run --host=0.0.0.0 --port=5000
```
Or using **Waitress (for production)**:
```sh
python -m waitress --port=5000 wsgi:app
```

### 7. Testing API Routes
Use Postman or `curl` to test API endpoints, e.g.:
```sh
curl -X GET http://127.0.0.1:5000/cryptos
```

### 8. Running in Debug Mode (for Development)
```sh
flask run --debug
```

### 9. Deploying on Production (Gunicorn + Nginx)
For production, use Gunicorn:
```sh
gunicorn -w 4 -b 0.0.0.0:5000 wsgi:app
```

### 10. Common Issues & Fixes
- **ModuleNotFoundError**: Ensure virtual environment is activated before running.
- **MongoDB Connection Errors**: Check if MongoDB service is running.
- **Port Already in Use**: Kill process using `lsof -i :5000` (macOS/Linux) or `netstat -ano | findstr :5000` (Windows).

---
Your Flask project is now set up! 🚀 Happy Coding!