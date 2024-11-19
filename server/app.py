
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import pandas as pd
import io
import os
from utils import *

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# MongoDB connection
try:
    # Get MongoDB URI from environment variables
    uri = os.getenv('MONGODB_URI')
    
    # Create MongoDB client with Server API
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    # Test connection with ping
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
    
    # Get database reference
    db = client[os.getenv('DB_NAME')]
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

# @app.route('/query', methods=['POST'])
# def query_data():
    
#     result = 'Hello'
#     #Logic to map user query to database function
#     #call that function and return output
    
#     return result


# Sample route
@app.route('/api/test', methods=['GET'])
def test_route():
    try:
        # Test MongoDB connection with a simple query
        test_collection = db['test']
        result = test_collection.find_one()
        return jsonify({
            "message": "API is working!",
            "mongodb_test": result
        }), 200
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

# Upload endpoint
# Test using curl:
# curl -X POST -F "file=@data/courses.csv" http://127.0.0.1:5000/api/upload
@app.route('/api/upload', methods=['POST'])
def upload_data():
    try:
        # Check if file is present in request
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        
        file = request.files['file']
        
        # Check if file has a name and is CSV
        if file.filename == '' or not file.filename.endswith('.csv'):
            return jsonify({"error": "Invalid file format. Please upload a CSV file"}), 400

        # Get collection name from filename (remove .csv extension)
        collection_name = file.filename.rsplit('.', 1)[0]
        
        # Read CSV file
        csv_data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))
        
        # Convert DataFrame to list of dictionaries
        records = csv_data.to_dict('records')
        
        # Insert into MongoDB
        collection = db[collection_name]
        collection.insert_many(records)
        
        return jsonify({
            "message": f"Successfully uploaded data to collection: {collection_name}",
            "rows_inserted": len(records)
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

@app.route('/api/query', methods=['POST'])
def query_data():
    try:
        # Get request data
        data = request.get_json()
        
        # Validate required fields
        if not data or 'query' not in data:
            return jsonify({
                "error": "Missing required fields. Please provide query string"
            }), 400
            
        query_str = data['query']
        
        try:
            collection_name, pipeline = extract_mongo_query(query_str)
        except ValueError as e:
            return jsonify({
                "error": str(e)
            }), 400
            
        # Get collection reference
        collection = db[collection_name]
        
        # Execute aggregation pipeline
        results = list(collection.aggregate(pipeline))
        
        # Convert ObjectId to string for JSON serialization
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        
        return jsonify({
            "results": results,
            "count": len(results)
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

        
if __name__ == '__main__':
    app.run(debug=True)