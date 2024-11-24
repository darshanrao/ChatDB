import re
import json

from rex import QueryER
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
        print("Kates function")
        return "Not coded"
    else:
        decode= QueryER()
        query = decode.decompose(query_str,dataschema=convert_schema_to_string(schema),database=database)
        
    return query


def convert_schema_to_string(schema_dict):
    import json
    
    # Use json.dumps with indent=2 and separators to control formatting
    schema_str = json.dumps(schema_dict, indent=2, separators=(',', ': '))
    
    # Remove newlines between array elements
    import re
    schema_str = re.sub(r'\[\n\s+', '[', schema_str)
    schema_str = re.sub(r',\n\s+', ', ', schema_str)
    schema_str = re.sub(r'\n\s+\]', ']', schema_str)
    
    # Wrap in triple quotes
    schema_str = f'"""\n{schema_str}\n"""'
    
    return schema_str