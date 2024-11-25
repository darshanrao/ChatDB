import re
import json

from rex import QueryER
from regex import *
def extract_mongo_query(query_str):
    """
    Extract collection name and pipeline array from MongoDB query string using custom parser
    
    Args:
        query_str (str): MongoDB aggregation query string
        
    Returns:
        tuple: (collection_name, pipeline_array)
    """
    def parse_value(value_str):
        """Parse a MongoDB value into Python object"""
        value_str = value_str.strip()
        
        # Handle numbers
        if value_str.isdigit():
            return int(value_str)
        try:
            return float(value_str)
        except ValueError:
            pass
            
        # Handle booleans
        if value_str.lower() == 'true':
            return True
        if value_str.lower() == 'false':
            return False
            
        # Handle null
        if value_str.lower() == 'null':
            return None
            
        # Remove quotes if string
        if value_str.startswith('"') or value_str.startswith("'"):
            return value_str[1:-1]
            
        return value_str

    def parse_object(obj_str):
        """Parse a MongoDB object into Python dict"""
        obj_str = obj_str.strip()
        if not obj_str.startswith('{') or not obj_str.endswith('}'):
            raise ValueError(f"Invalid object format: {obj_str}")
            
        result = {}
        # Remove curly braces
        content = obj_str[1:-1].strip()
        
        if not content:
            return result
            
        # Split by commas, but handle nested structures
        parts = []
        current = []
        depth = 0
        
        for char in content:
            if char in '{[':
                depth += 1
            elif char in '}]':
                depth -= 1
            elif char == ',' and depth == 0:
                parts.append(''.join(current).strip())
                current = []
                continue
            current.append(char)
        
        if current:
            parts.append(''.join(current).strip())
            
        # Process each key-value pair
        for part in parts:
            if ':' not in part:
                raise ValueError(f"Invalid key-value pair: {part}")
            key, value = part.split(':', 1)
            key = key.strip()
            
            # Remove quotes from key if present
            if key.startswith('"') or key.startswith("'"):
                key = key[1:-1]
                
            # Parse the value
            value = value.strip()
            if value.startswith('{'):
                value = parse_object(value)
            elif value.startswith('['):
                value = parse_array(value)
            else:
                value = parse_value(value)
                
            result[key] = value
            
        return result

    def parse_array(array_str):
        """Parse a MongoDB array into Python list"""
        array_str = array_str.strip()
        if not array_str.startswith('[') or not array_str.endswith(']'):
            raise ValueError(f"Invalid array format: {array_str}")
            
        # Remove brackets
        content = array_str[1:-1].strip()
        
        if not content:
            return []
            
        # Split by commas, but handle nested structures
        parts = []
        current = []
        depth = 0
        
        for char in content:
            if char in '{[':
                depth += 1
            elif char in '}]':
                depth -= 1
            elif char == ',' and depth == 0:
                parts.append(''.join(current).strip())
                current = []
                continue
            current.append(char)
        
        if current:
            parts.append(''.join(current).strip())
            
        # Parse each value
        result = []
        for part in parts:
            part = part.strip()
            if part.startswith('{'):
                value = parse_object(part)
            elif part.startswith('['):
                value = parse_array(part)
            else:
                value = parse_value(part)
            result.append(value)
            
        return result

    # Extract collection name
    collection_match = re.match(r'db\.(\w+)\.aggregate', query_str)
    if not collection_match:
        raise ValueError("Invalid query format. Must start with db.collection.aggregate")
    
    collection_name = collection_match.group(1)
    
    # Extract and parse pipeline array
    pipeline_match = re.search(r'\[(.*)\]', query_str, re.DOTALL)
    if not pipeline_match:
        raise ValueError("Invalid query format. Must contain aggregation pipeline array")
    
    pipeline = parse_array(pipeline_match.group(0))
    
    return collection_name, pipeline
        
def query_generator(query_str, schema, database,option):
    
    if option == 0:
        if database == "sql":
            query =  query_function_sql(schema, query_str)
        elif database == "mongodb":
            query = sql_to_mongo(query_function_sql(data_schema=schema,query=query_str))
            # import pdb; pdb.set_trace()
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