import re
import json


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