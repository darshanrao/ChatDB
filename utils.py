import re
import json

from rex import QueryER
from regex import *
def extract_mongo_query(query_str):
    """
    Extract collection name and pipeline array from MongoDB query string
    
    Args:
        query_str (str): MongoDB aggregation query string
        
    Returns:
        tuple: (collection_name, pipeline_array)
        
    Raises:
        ValueError: If query string format is invalid
    """
    # Extract collection name
    collection_match = re.match(r'db\.(\w+)\.aggregate', query_str)
    if not collection_match:
        raise ValueError("Invalid query format. Must start with db.collection.aggregate")
        
    collection_name = collection_match.group(1)
    
    # Extract pipeline array
    pipeline_str = re.search(r'\[(.*)\]', query_str, re.DOTALL)
    if not pipeline_str:
        raise ValueError("Invalid query format. Must contain aggregation pipeline array")
        
    # Parse pipeline string to Python object
    try:
        pipeline = json.loads(f"[{pipeline_str.group(1)}]")
    except json.JSONDecodeError:
        raise ValueError("Invalid pipeline format. Must be valid JSON")
        
    return collection_name, pipeline

def query_generator(query_str, schema, database,option):
    
    if option == 0:
        if database == "sql":
            query =  query_function_sql(schema, query_str)
        # elif database == "mongodb":
            # query = query_function_mongodb(schema,query_str)
    else:
        decode= QueryER()
        query = decode.decompose(query_str,dataschema=convert_schema_to_string(schema),database=database)
    return query


def convert_schema_to_string(schema_dict):

    schema_str = json.dumps(schema_dict, indent=2, separators=(',', ': '))
    schema_str = re.sub(r'\[\n\s+', '[', schema_str)
    schema_str = re.sub(r',\n\s+', ', ', schema_str)
    schema_str = re.sub(r'\n\s+\]', ']', schema_str)

    schema_str = f'"""\n{schema_str}\n"""'
    
    return schema_str