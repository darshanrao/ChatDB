
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


app = Flask(__name__, 
    template_folder='templates',  # Add template folder
    static_folder='static'        # Add static folder for CSS/JS
)
CORS(app)



# connection = None

# try:
#         connection = pymysql.connect(
#             host=os.getenv('RDS_HOST'),
#         port=int(os.getenv('RDS_PORT')), 
#             user=os.getenv('RDS_USER'),
#             password=os.getenv('RDS_PASSWORD'),
#             database=os.getenv('RDS_DATABASE'),
#             connect_timeout=10,  # Add timeout parameter
#             read_timeout=10,
#             write_timeout=10
#         )
#         print("Connected to RDS successfully!")
# except pymysql.Error as e:
#     print(f"Could not connect to database: {e}")


def get_RDS_database_connection():
    try:
        connection = pymysql.connect(
            host=os.getenv('RDS_HOST'),
            port=int(os.getenv('RDS_PORT')), 
            user=os.getenv('RDS_USER'),
            password=os.getenv('RDS_PASSWORD'),
            database=os.getenv('RDS_DATABASE'),
            connect_timeout=10,
            read_timeout=10,
            write_timeout=10
        )
        print("Connected to RDS successfully!")
        return connection
    except pymysql.Error as e:
        print(f"Could not connect to database: {e}")
        raise

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

        connection = get_RDS_database_connection()
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
            connection.close()
            return jsonify({
                "message": f"Successfully uploaded data to table: {table_name}",
                "rows_inserted": len(df)
            }), 200

        except pymysql.Error as e:
            connection.rollback()
            connection.close()
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

        connection = get_RDS_database_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(query_str)
                
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]

                connection.close()
                return jsonify({
                    "results": results, 
                    "count": len(results)
                }), 200

        except pymysql.Error as e:
            connection.close()
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
        connection = get_RDS_database_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 10")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                data = [dict(zip(columns, row)) for row in rows]

                connection.close()
                return jsonify({
                    "table_name": table_name,
                    "total_rows": count,
                    "sample_data": data
                }), 200
                
        except pymysql.Error as e:
            connection.close()
            return jsonify({"error": f"Database error: {str(e)}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
def connect_mongodb(db_name):
    try:
        uri = os.getenv('MONGODB_URI')
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        return client, client[db_name]
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise e


# Upload endpoint
# Test using curl:
"""
curl -X POST \
  -F "db_name=database2" \
  -F "files=@data/courses.csv" \
  -F "files=@data/enrollments.csv" \
  -F "files=@data/students.csv" \
  http://127.0.0.1:5000/api/upload-mongodb
"""
@app.route('/api/upload-mongodb', methods=['POST']) 
def upload_data():
    client = None
    try:
        if 'files' not in request.files:
            return jsonify({"error": "No files uploaded"}), 400
        
        if 'db_name' not in request.form:
            return jsonify({"error": "Database name not provided"}), 400

        files = request.files.getlist('files')
        db_name = request.form['db_name']
        
        if not files:
            return jsonify({"error": "No files selected"}), 400

        # Connect to MongoDB with specified database
        client, db = connect_mongodb(db_name)
        
        upload_results = []
        
        for file in files:
            if file.filename == '' or not file.filename.endswith('.csv'):
                upload_results.append({
                    "filename": file.filename,
                    "status": "error",
                    "message": "Invalid file format. Please upload a CSV file"
                })
                continue

            try:
                collection_name = file.filename.rsplit('.', 1)[0]
                csv_data = pd.read_csv(io.StringIO(file.stream.read().decode("UTF8")))
                records = csv_data.to_dict('records')

                collection = db[collection_name]
                collection.insert_many(records)
                
                upload_results.append({
                    "filename": file.filename,
                    "status": "success",
                    "collection": collection_name,
                    "rows_inserted": len(records)
                })
                
            except Exception as e:
                upload_results.append({
                    "filename": file.filename,
                    "status": "error",
                    "message": str(e)
                })
        
        return jsonify({
            "message": f"Upload process completed to database: {db_name}",
            "results": upload_results
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
    finally:
        if client:
            client.close()
            print("MongoDB connection closed")

# Executes MongoDB aggregation pipeline query and returns results            
"""
curl -X POST http://127.0.0.1:5000/api/query-mongodb \
-H "Content-Type: application/json" \
-d '{
    "db_name": "database2",
    "query": "db.students.aggregate([{ \"$group\": { \"_id\": \"$Major\", \"studentCount\": { \"$sum\": 1 } } }])"
}'
"""
@app.route('/api/query-mongodb', methods=['POST'])
def query_data():
    client = None
    try:
        data = request.get_json()

        if not data or 'query' not in data or 'db_name' not in data:
            return jsonify({
                "error": "Missing required fields. Please provide db_name and query string"
            }), 400
            
        # Remove escaped quotes if present
        query_str = data['query'].replace('\\"', '"')
        db_name = data['db_name']
        
        try:
            collection_name, pipeline = extract_mongo_query(query_str) 
        except ValueError as e:
            return jsonify({
                "error": str(e)
            }), 400
        
        # Connect to MongoDB with specified database    
        client, db = connect_mongodb(db_name)
            
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
    finally:
        if client:
            client.close()
            print("MongoDB connection closed")
            

def get_collections_schema(db_name):
    client = None
    try:
        client, db = connect_mongodb(db_name)
        collections_schema = {}
        
        # Get all collections in the database
        for collection_name in db.list_collection_names():
            # Get one document from collection to extract fields
            sample_doc = db[collection_name].find_one()
            if sample_doc:
                # Remove _id field and get all column names
                sample_doc.pop('_id', None)
                collections_schema[collection_name] = list(sample_doc.keys())
            
        return collections_schema
        
    except Exception as e:
        print(f"Error getting schema: {e}")
        raise e
    finally:
        if client:
            client.close()
            print("MongoDB connection closed")

"""
curl -X GET http://127.0.0.1:5000/api/get-mongodb-schema/database2
"""
@app.route('/api/get-mongodb-schema/<db_name>', methods=['GET'])
def get_schema(db_name):
    try:
        schema = get_collections_schema(db_name)
        return jsonify({
            "database": db_name,
            "collections": schema
        }), 200
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500


from flask import Flask, jsonify, request, render_template        
# Add route for the main page
@app.route('/')
def index():
    return render_template('chatdb.html')

if __name__ == '__main__':
    app.run(debug=True)