import re
import json
import random
from rex import QueryER
from regex import *
from string import Template


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

def generate_sample_queries(schema, operation=None, db='sql'):
    """
    Generates sample queries based on the database schema and requested operation.
    
    :param schema: Dictionary containing table names as keys and a list of column metadata as values.
                   e.g., {"sales": [{"name": "amount", "type": "int"}, {"name": "category", "type": "string"}]}
    :param operation: Specific operation to generate queries for (e.g., "group by", "aggregate").
                      If None, generates general sample queries.
    :return: A list of tuples, each containing a natural language description and its corresponding SQL query.
    """
    sample_queries = []
    if db=='mysql':
        query_templates = {
            "group by": "SELECT {quantitative}, {categorical}, COUNT(*) AS count FROM {table} GROUP BY {categorical};",
            "aggregate": "SELECT SUM({quantitative}) AS total_{quantitative}, AVG({quantitative}) AS avg_{quantitative} FROM {table};",
            "basic": "SELECT * FROM {table} LIMIT 10;",
            "filter": "SELECT * FROM {table} WHERE {condition};",
            "join": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} INNER JOIN {table2} ON {table1}.{key} = {table2}.{key};",
            "order by": "SELECT * FROM {table} ORDER BY {column} {order};",
            "insert": "INSERT INTO {table} ({columns}) VALUES ({values});",
            "update": "UPDATE {table} SET {column} = {value} WHERE {condition};",
            "delete": "DELETE FROM {table} WHERE {condition};",
            "create table": "CREATE TABLE {table} ({columns});",
            "alter table": "ALTER TABLE {table} ADD COLUMN {column} {datatype};",
            "drop table": "DROP TABLE {table};",
            "count rows": "SELECT COUNT(*) AS row_count FROM {table};",
            "distinct": "SELECT DISTINCT {column} FROM {table};",
            "having": "SELECT {quantitative}, {categorical} FROM {table} GROUP BY {categorical} HAVING COUNT(*) > {value};",
            "union": "SELECT {columns} FROM {table1} UNION SELECT {columns} FROM {table2};",
            "limit": "SELECT * FROM {table} LIMIT {number};",
            "like": "SELECT * FROM {table} WHERE {column} LIKE '{pattern}';",
            "between": "SELECT * FROM {table} WHERE {column} BETWEEN {value1} AND {value2};",
            "exists": "SELECT * FROM {table} WHERE EXISTS (SELECT 1 FROM {subquery});",
        }
        
        nl_templates = {
            "group by": "Count of rows grouped by {categorical} in the {table} table.",
            "aggregate": "Total and average of {quantitative} in the {table} table.",
            "basic": "Preview the first 10 rows of the {table} table.",
            "join": "Retrieve rows by joining {table1} and {table2} on {join_condition}.",
            "filter": "Retrieve rows from {table} where a {condition}.",
            "order by": "Retrieve rows from {table} sorted by {column} in {order} order.",
            "insert": "Insert a new record into the {table} table with values {values}.",
            "update": "Update records in the {table} table where {condition} to set {update_values}.",
            "delete": "Delete records from the {table} table where {condition}.",
            "having": "Filter grouped results in the {table} table having {condition}.",
            "distinct": "Retrieve unique values from {column} in the {table} table.",
            "count": "Count the number of rows in the {table} table.",
            "create table": "Create a new table named {table} with specified columns.",
            "alter table": "Modify the structure of the {table} table by adding, modifying, or dropping columns.",
            "drop table": "Remove the {table} table from the database.",
            "union": "Combine the results of two queries, removing duplicate rows.",
            "limit": "Retrieve only the first {limit_value} rows from the {table} table.",
            "like": "Search for rows in {table} where {column} matches the pattern {pattern}.",
            "between": "Retrieve rows from {table} where {column} values are between {value1} and {value2}.",
            "exists": "Check if a subquery returns any rows in relation to the {table} table.",
        }


        for table, columns in schema.items():
            quantitative_columns = [col["name"] for col in columns if col["type"] in ["int", "float"]]
            categorical_columns = [col["name"] for col in columns if col["type"] in ["string", "varchar", "text"]]
            all_columns = [col["name"] for col in columns]

            if operation == "group by" and quantitative_columns and categorical_columns:
                query = query_templates["group by"].format(
                    quantitative=random.choice(quantitative_columns),
                    categorical=random.choice(categorical_columns),
                    table=table
                )
                description = nl_templates["group by"].format(
                    categorical=random.choice(categorical_columns),
                    table=table
                )
                sample_queries.append((description, query))

            elif operation == "aggregate" and quantitative_columns:
                query = query_templates["aggregate"].format(
                    quantitative=random.choice(quantitative_columns),
                    table=table
                )
                description = nl_templates["aggregate"].format(
                    quantitative=random.choice(quantitative_columns),
                    table=table
                )
                sample_queries.append((description, query))

            elif operation == "filter" and all_columns:
                query = query_templates["filter"].format(
                    table=table,
                    condition=f"{random.choice(all_columns)} = 'some_value'"
                )
                description = nl_templates["filter"].format(table=table, condition="condition")

                sample_queries.append((description, query))

            elif operation == "join" and len(schema) > 1:
                other_table = random.choice([t for t in schema if t != table])
                table1_key = random.choice(all_columns)
                table2_key = random.choice([col["name"] for col in schema[other_table]])
                query = query_templates["join"].format(
                    table1=table,
                    table2=other_table,
                    column1=random.choice(all_columns),
                    column2=random.choice([col["name"] for col in schema[other_table]]),
                    key=table1_key
                )
                description = nl_templates["join"].format(table1=table, table2=other_table, key=table1_key)

                sample_queries.append((description, query))

            elif operation == "order by" and all_columns:
                ord = random.choice(["ASC", "DESC"])
                col = random.choice(all_columns)
                query = query_templates["order by"].format(
                    table=table,
                    column=col,
                    order=ord
                )
                description = nl_templates["order by"].format(
                    table=table, column=col, order=ord
                )
                sample_queries.append((description, query))

            elif operation == "insert" and all_columns:
                columns = random.sample(all_columns, min(3, len(all_columns)))
                values = ["'value'" for _ in columns]
                query = query_templates["insert"].format(
                    table=table,
                    columns=", ".join(columns),
                    values=", ".join(values)
                )
                description = nl_templates["insert"].format(table=table, values=", ".join(values))
                sample_queries.append((description, query))

            elif operation == "update" and all_columns:
                query = query_templates["update"].format(
                    table=table,
                    column=random.choice(all_columns),
                    value="'new_value'",
                    condition=f"{random.choice(all_columns)} = 'some_value'"
                )
                description = nl_templates["update"].format(table=table, condition=f"{random.choice(all_columns)} = 'some_value'", update_values= "new value")
                sample_queries.append((description, query))

            elif operation == "delete" and all_columns:
                query = query_templates["delete"].format(
                    table=table,
                    condition=f"{random.choice(all_columns)} = 'some_value'"
                )
                description = nl_templates["delete"].format(table=table, condition="condition")
                sample_queries.append((description, query))

            elif operation == "count rows":
                query = query_templates["count rows"].format(table=table)
                description = nl_templates["count rows"].format(table=table)
                sample_queries.append((description, query))

            elif operation == "distinct" and categorical_columns:
                col = random.choice(categorical_columns)
                query = query_templates["distinct"].format(
                    table=table,
                    column=col
                )
                description = nl_templates["distinct"].format(
                    table=table, column=col
                )
                sample_queries.append((description, query))

            elif operation == "limit":
                query = query_templates["limit"].format(
                    table=table,
                    number=random.randint(1, 10)
                )
                description = nl_templates["limit"].format(table=table, limit_value=random.randint(1, 10))
                sample_queries.append((description, query))

            elif operation == "like" and categorical_columns:
                col = random.choice(categorical_columns)
                query = query_templates["like"].format(
                    table=table,
                    column=col,
                    pattern="pattern%"
                )
                description = nl_templates["like"].format(
                    table=table, column=col, pattern="pattern%"
                )
                sample_queries.append((description, query))

            elif operation == "between" and quantitative_columns:
                col = random.choice(quantitative_columns)
                query = query_templates["between"].format(
                    table=table,
                    column=col,
                    value1=random.randint(1, 100),
                    value2=random.randint(101, 200)
                )
                description = nl_templates["between"].format(
                    table=table, column=col, value1=10, value2=100
                )
                sample_queries.append((description, query))

            elif operation == "exists":
                query = query_templates["exists"].format(
                    table=table,
                    subquery=f"SELECT 1 FROM {random.choice(list(schema.keys()))} WHERE some_column = some_value"
                )
                description = nl_templates["exists"].format(table=table)
                sample_queries.append((description, query))

            elif operation == "mysql":  # Basic query
                query = query_templates["basic"].format(table=table)
                description = nl_templates["basic"].format(table=table)
                sample_queries.append((description, query))

        return sample_queries
    else:
        sample_queries = []
        
        query_templates = {
            "aggregate": '[{"$group": {"_id": "$${categorical}", "total": {"$sum": "$${quantitative}"}}}]',
            "find": '{"$match": {"${column}": "${value}"}}',
            "count": '[{"$count": "total"}]',
            "distinct": '[{"$distinct": "${column}"}]',
            "limit": '[{"$limit": "${number}"}]',
            "update": '{"$set": {"${column}": "${value}"}}',
            "delete": '{"$delete": {"${column}": "${value}"}}',
            "group by": '[{"$group": {"_id": "$${categorical}", "count": {"$sum": 1}}}]',
        }
        
        nl_templates = {
            "aggregate": "Aggregate data by summing up the {quantitative} field and grouping by {categorical}.",
            "find": "Find records where {column} is equal to {value}.",
            "count": "Get the total count of records.",
            "distinct": "Get distinct values from the {column} field.",
            "limit": "Get a limited number of records, up to {number}.",
            "update": "Update the {column} field to {value} for matching records.",
            "delete": "Delete records where {column} is equal to {value}.",
            "group by": "Group records by {categorical} and count the number of occurrences.",
        }
        
        for table, columns in schema.items():
            quantitative_columns = [col["name"] for col in columns if col["type"] in ["int", "float"]]
            categorical_columns = [col["name"] for col in columns if col["type"] in ["string", "varchar", "text"]]

            if operation == "group by" and quantitative_columns and categorical_columns:
                query = query_templates["group by"].replace('${categorical}', random.choice(categorical_columns))

                description = nl_templates["group by"].format(
                    categorical=random.choice(categorical_columns),
                )
                sample_queries.append((description, query))

            elif operation == "aggregate" and quantitative_columns:
                query = query_templates["aggregate"].replace("${categorical}", random.choice(categorical_columns))
                query = query.replace("${quantitative}", random.choice(quantitative_columns))
                description = nl_templates["aggregate"].format(
                    quantitative=random.choice(quantitative_columns),
                    categorical=random.choice(categorical_columns),
                )
                sample_queries.append((description, query))

            elif operation == "count":
                query = query_templates["count"]
                description = nl_templates["count"]
                sample_queries.append((description, query))

            elif operation == "distinct" and categorical_columns:
                query = query_templates["distinct"].replace("${column}", random.choice(categorical_columns))
                
                description = nl_templates["distinct"].format(
                    column=random.choice(categorical_columns),
                )
                sample_queries.append((description, query))

            elif operation == "limit":
                query = query_templates["limit"].replace("${number}", "10")
                description = nl_templates["limit"].format( number=10)
                sample_queries.append((description, query))

            elif operation == "update" and categorical_columns:
                query = query_templates["update"].replace("${column}", random.choice(categorical_columns))
                query = query.replace("${value}", random.randint(1, 100))
                description = nl_templates["update"].format(
                    column=random.choice(categorical_columns),
                    value=random.randint(1, 100),
                )
                sample_queries.append((description, query))

            elif operation == "delete" and categorical_columns:
                query = query_templates["delete"].replace("${column}", random.choice(categorical_columns))
                query = query.replace("${value}", random.choice(['value1', 'value2']))
                description = nl_templates["delete"].format(
                    column=random.choice(categorical_columns),
                    value=random.choice(['value1', 'value2']),
                )
                sample_queries.append((description, query))

            elif operation == "mongodb":
                query = query_templates["find"].replace("${column}", "name").replace("${value}", "John")
                description = nl_templates["find"].format(column="name", value="John")
                sample_queries.append((description, query))

            
        return sample_queries