
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import pandas as pd
import io
import os
from utils import *

import time
import pymysql
from sqlalchemy import create_engine



load_dotenv()


app = Flask(__name__)
CORS(app)



connection = None

try:
        connection = pymysql.connect(
            host=os.getenv('RDS_HOST'),
        port=int(os.getenv('RDS_PORT')), 
            user=os.getenv('RDS_USER'),
            password=os.getenv('RDS_PASSWORD'),
            database=os.getenv('RDS_DATABASE'),
            connect_timeout=10,  # Add timeout parameter
            read_timeout=10,
            write_timeout=10
        )
        print("Connected to RDS successfully!")
except pymysql.Error as e:
    print(f"Could not connect to database: {e}")

# Uploads CSV file data to MySQL RDS database
# curl -X POST -F "file=@data/courses.csv" http://127.0.0.1:5000/api/upload-mysql
@app.route('/api/upload-mysql', methods=['POST'])
def upload_to_rds():
    try:

        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        
        file = request.files['file']
    
        if file.filename == '' or not file.filename.endswith('.csv'):
            return jsonify({"error": "Invalid file format. Please upload a CSV file"}), 400

        table_name = file.filename.rsplit('.', 1)[0]


        df = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))

        columns = []
        for column, dtype in df.dtypes.items():
            if dtype == 'int64':
                sql_type = 'INT'
            elif dtype == 'float64':
                sql_type = 'FLOAT'
            else:
                sql_type = 'VARCHAR(255)'
            columns.append(f"`{column}` {sql_type}")
            
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)}
        )
        """

        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"

        try:
            with connection.cursor() as cursor:
    
                cursor.execute(create_table_query)
                
                cursor.executemany(insert_query, df.values.tolist())
                
                connection.commit()

            return jsonify({
                "message": f"Successfully uploaded data to table: {table_name}",
                "rows_inserted": len(df)
            }), 200

        except pymysql.Error as e:
            connection.rollback()
            return jsonify({"error": f"Database error: {str(e)}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Sample curl command
"""
curl -X POST http://127.0.0.1:5000/api/query-mysql \
  -d "SELECT Major, COUNT(*) as studentCount FROM students GROUP BY Major"
"""
    
@app.route('/api/query-mysql', methods=['POST'])
def query_mysql():
    try:
        # Get raw data from request body
        query_str = request.get_data(as_text=True)

        if not query_str:
            return jsonify({
                "error": "Missing query string in request body"
            }), 400

        with connection.cursor() as cursor:
            cursor.execute(query_str)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            
            return jsonify({
                "results": results,
                "count": len(results)
            }), 200
            
    except pymysql.Error as e:
        return jsonify({
            "error": f"Database error: {str(e)}"
        }), 500
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
@app.route('/api/tables-mysql/<table_name>', methods=['GET'])
def get_table_data(table_name):
    try:
        with connection.cursor() as cursor:

            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 10")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            data = [dict(zip(columns, row)) for row in rows]
            
            return jsonify({
                "table_name": table_name,
                "total_rows": count,
                "sample_data": data
            }), 200
            
    except pymysql.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    
# MongoDB connection
try:

    uri = os.getenv('MONGODB_URI')

    client = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
    
    db = client[os.getenv('DB_NAME')]
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")



# Upload endpoint
# Test using curl:
# curl -X POST -F "file=@data/courses.csv" http://127.0.0.1:5000/api/upload-mongodb
@app.route('/api/upload-mongodb', methods=['POST'])
def upload_data():
    try:

        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        
        file = request.files['file']
        
        if file.filename == '' or not file.filename.endswith('.csv'):
            return jsonify({"error": "Invalid file format. Please upload a CSV file"}), 400

        collection_name = file.filename.rsplit('.', 1)[0]

        csv_data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))
        
        records = csv_data.to_dict('records')

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

# Executes MongoDB aggregation pipeline query and returns results        
"""
curl -X POST http://127.0.0.1:5000/api/query-mongodb \
-d 'db.students.aggregate([{ "$group": { "_id": "$Major", "studentCount": { "$sum": 1 } } }])'
"""
@app.route('/api/query-mongodb', methods=['POST'])
def query_data():
    try:
        data = request.get_data().decode('utf-8')

        if not data:
            return jsonify({
                "error": "Missing required fields. Please provide query string"
            }), 400
            
        # Remove escaped quotes if present
        query_str = data.replace('\\"', '"')
        
        try:
            collection_name, pipeline = extract_mongo_query(query_str) 
        except ValueError as e:
            return jsonify({
                "error": str(e)
            }), 400
            
        collection = db[collection_name]
        
        results = list(collection.aggregate(pipeline))
        
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