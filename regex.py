
import pandas as pd
import glob

from collections import defaultdict
import os
import re


import textwrap
def check_join_needed_func(schema_dict,query):
    """
    Checks if the query columns are spread across multiple CSV files using a predefined schema dictionary.

    Parameters:
        query (str): User query containing column names and random words.
        schema_dict (dict): Dictionary mapping table names to their columns.

    Returns:
        bool: True if a join is needed (columns spread across multiple files), False otherwise.
    """
    # Extract column names from the query
    query_columns = set(query.split())
    
    # Track which tables contain the columns we're looking for
    tables_with_columns = []
    
    # For each column in the query, find which table contains it
    for column in query_columns:
        for table, columns in schema_dict.items():
            if column in columns:
                tables_with_columns.append(table)
                break
    
    # Check if columns are spread across multiple tables
    unique_tables = set(tables_with_columns)
    
    join_needed = len(unique_tables) > 1
    return join_needed


def build_column_table_mapping(schema_dict):
    """
    Converts schema dictionary into column to table mapping.
    :param schema_dict: Dictionary containing table names as keys and list of columns as values 
    :return: Dictionary mapping column names to table names and dictionary of table dataframes
    """
    mapping = defaultdict(list)
    tables = {}
    
    # Build column -> table mapping from schema
    for table_name, columns in schema_dict.items():
        # Create empty dataframe with schema columns
        tables[table_name] = pd.DataFrame(columns=columns)
        for column in columns:
            mapping[column].append(table_name)
            
    return mapping, tables

def query_function_sql(data_schema, query):

    # csv_folder = directory 

    sample_queries = [query]

    test_queries = [query]



    # import pdb; pdb.set_trace()

    check_join_needed = check_join_needed_func(data_schema, query)

    if check_join_needed == True:

        print("Join")

        
        def auto_generate_query(raw_query, column_table_mapping, tables):
            """
            Automatically resolves and rewrites the query by determining joins and conditions based on column-table mapping.
            :param raw_query: User-specified natural language query.
            :param column_table_mapping: Dictionary mapping columns to tables.
            :param tables: Dictionary containing table dataframes for column checks.
            :return: Rewritten SQL query with compact formatting.
            """
            # Define a single regex pattern using reusable keywords
            KEYWORDS_PATTERN = r"(?:find|list|determine|show|get|retrieve|give me|provide|display|fetch|what are|show me)?\s*"
            CONTEXT_PATTERN = r"(?:from|in|on|of)?\s*(?:context)?"
            QUERY_PATTERN = re.compile(
                rf"{KEYWORDS_PATTERN}(.+?)\s*{CONTEXT_PATTERN}\s*(?:where|if|with|satisfying|that (?:meet|fulfill))\s+(.+)",
                re.IGNORECASE,
            )

            # SQL keywords to normalize
            SQL_KEYWORDS = ["between", "like", "is not null", "is null", "and", "or", "not", "in"]

            # Stop words to remove
            STOP_WORDS = {"a", "an", "the", "and"}

            # Preprocess raw query to remove stop words
            def preprocess_query(query):
                words = query.split()
                return " ".join(word for word in words if word.lower() not in STOP_WORDS)

            # Preprocess the query
            raw_query = preprocess_query(raw_query)

            # Match the query
            match = QUERY_PATTERN.match(raw_query)
            if not match:
                raise ValueError("Query format is invalid. Please use a supported structure.")

            selected_columns = match.group(1).split(", ")
            condition = match.group(2).strip()

            # Resolve tables for selected columns
            columns_with_tables = []
            required_tables = set()
            for column in selected_columns:
                if column in column_table_mapping:
                    table_name = column_table_mapping[column][0]  # Take the first match
                    columns_with_tables.append(f"{table_name}.{column}")
                    required_tables.add(table_name)
                else:
                    raise ValueError(f"Column '{column}' not found in any table.")

            # Normalize SQL keywords in the condition
            for keyword in SQL_KEYWORDS:
                condition = re.sub(rf"\b{keyword}\b", keyword.upper(), condition, flags=re.IGNORECASE)

            # Determine join conditions
            join_conditions = []
            required_tables = list(required_tables)
            for i in range(len(required_tables) - 1):
                table_a, table_b = required_tables[i], required_tables[i + 1]
                common_columns = set(tables[table_a].columns).intersection(tables[table_b].columns)

                # Use only one common column for a simple join
                if len(common_columns) < 1:
                    raise ValueError(f"No common column found to join '{table_a}' and '{table_b}'.")

                common_column = next(iter(common_columns))  # Take the first common column
                join_conditions.append(f"{table_a}.{common_column} = {table_b}.{common_column}")

            # Construct the SQL query
            formatted_query = f"SELECT {', '.join(columns_with_tables)}\nFROM {required_tables[0]}"
            for i in range(1, len(required_tables)):
                formatted_query += f"\nJOIN {required_tables[i]} ON {join_conditions[i - 1]}"
            formatted_query += f"\nWHERE {condition};"

            return formatted_query.strip()

        column_table_mapping, tables = build_column_table_mapping(data_schema)


        # Process sample queries
        for query in sample_queries:
            try:
                resolved_query = auto_generate_query(query, column_table_mapping, tables)
                return resolved_query
                # print(f"Input: {query}\nResolved Query:\n{resolved_query}\n")
            except ValueError as e:
                print(f"Error processing query '{query}': {e}\n")

    elif check_join_needed == False:

        print("No Join")



        # Define templates with proper indentation using textwrap.dedent
        query_templates = [
        
            # Count entries in a table
            (
                r"(?:find|count)?\s*entries in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) AS entry_count
                    FROM {m.group(1)};
                """).strip()
            ),

            # Find all unique values
            (
                r"(?:find|list)?\s*unique (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT DISTINCT {m.group(1)}
                    FROM {m.group(2)};
                """).strip()
            ),
        ]

        #KEYWORDS_PATTERN = r"(?:find|list|determine|show|get|retrieve|give me\s*)?"

        query_templates += [
            # Sum of a column
            (
                r"(?:find|calculate)?\s*sum of (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT SUM({m.group(1)}) AS total_sum
                    FROM {m.group(2)};
                """).strip()
            ),

            # Average of a column
            (
                r"(?:find|calculate)?\s*average of (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT AVG({m.group(1)}) AS average_value
                    FROM {m.group(2)};
                """).strip()
            ),

            # Minimum value in a column
            (
                r"(?:find|list)?\s*minimum (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT MIN({m.group(1)}) AS min_value
                    FROM {m.group(2)};
                """).strip()
            ),

            # Maximum value in a column
            (
                r"(?:find|list)?\s*maximum (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT MAX({m.group(1)}) AS max_value
                    FROM {m.group(2)};
                """).strip()
            ),

            # Count distinct values in a column
            (
                r"(?:find|count)?\s*distinct (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(DISTINCT {m.group(1)}) AS distinct_count
                    FROM {m.group(2)};
                """).strip()
            ),

            # Find all rows where a column equals a value
            (
                r"(?:find|list)?\s*rows where (.+) equals (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} = {m.group(2)};
                """).strip()
            ),

            # Find all rows where a column is greater than a value
            (
                r"(?:find|list)?\s*rows where (.+) greater than (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} > {m.group(2)};
                """).strip()
            ),

            # Find all rows where a column is less than a value
            (
                r"(?:find|list)?\s*rows where (.+) less than (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} < {m.group(2)};
                """).strip()
            ),

            # Count rows with a specific condition
            (
                r"(?:find|count)?\s*rows where (.+) equals (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) AS row_count
                    FROM {m.group(3)}
                    WHERE {m.group(1)} = {m.group(2)};
                """).strip()
            ),
            
            
            # TEMPLATE 1: top N rows 
            # List top N rows in table
            
            # List top 5 rows in table_name
            # List top 10 rows in Students
            
            (
                r"(?:find|list)?\s*top (\d+) rows in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(2)}
                    LIMIT {m.group(1)};
                """).strip()
            ),
            
            
            
            # TEMPLATE 2: top N rows ordered by a column
            # List top N rows ordered by a column in table
            
            # List top 5 rows ordered by column_name in table_name
            # List top 10 rows ordered by CreditHours in Courses
            
            (
                r"(?:find|list)?\s*top (\d+) rows ordered by (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    ORDER BY {m.group(2)} 
                    LIMIT {m.group(1)};
                """).strip()
            ),
        ]
        
        query_templates += [
            
            # TEMPLATE 3: total number of rows
            # Counts the total number of rows in a table
            
            # Count total number of rows in table_name
            # Count total number of rows in Students
            (
                r"(?:find|count)?\s*total number of rows in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) 
                    FROM {m.group(1)};
                """).strip()
            ),
            
            
           
            # TEMPLATE 4: LIKE (starts with)
            # Lists rows where a column starts with a specific value
            
            # List rows where column_name starts with '...' in table_name
            # List rows where FirstName starts with 'A' in Students
            (
                r"(?:find|list)?\s*rows where (.+) starts with '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '{m.group(2)}%';
                """).strip()
            ),
            
            
            # TEMPLATE 5: COUNT+LIKE (starts with)
            # Lists rows where a column starts with a specific value
            
            # Count rows where column_name starts with '...' in table_name
            # Count rows where FirstName starts with 'A' in Students
            (
                r"(?:count)?\s*rows where (.+) starts with '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*)
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '{m.group(2)}%';
                """).strip()
            ),
            
            
            
            # TEMPLATE 6: LIKE (ends with)
            # Lists rows where a column ends with a specific value
            
            # List rows where column_name ends with '...' in table_name
            # List rows where FirstName ends with 'a' in Students
            (
                r"(?:find|list)?\s*rows where (.+) ends with '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '%{m.group(2)}';
                """).strip()
            ),
            
            
            # TEMPLATE 7: COUNT+LIKE (ends with)
            
            # Counts rows where a column ends with a specific value
            
            # Count rows where column_name ends with '...' in table_name
            # Count rows where FirstName ends with 'a' in Students
            (
                r"(?:count)?\s*rows where (.+) ends with '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*)
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '%{m.group(2)}';
                """).strip()
            ),
            
            
            # TEMPLATE 8: RANGE(MAX-MIN)
            # Calculates the range (max - min) of a column
            
            # Calculate range of column_name in table_name
            # Calculate range of CreditHours in Courses
            (
                r"(?:find|calculate)?\s*range of (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT MAX({m.group(1)}) - MIN({m.group(1)}) 
                    FROM {m.group(2)};
                """).strip()
            ),
            
            
            # TEMPLATE 9: BETWEEN
            # Lists rows where a column value is between two values
            
            # List rows where column_name value is between 1 and 3 in table_name
            # List rows where CreditHours value is between 5 and 7 in Courses
            
            (
                r"(?:find|list)?\s*rows where (.+) between (.+) and (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(4)}
                    WHERE {m.group(1)} BETWEEN {m.group(2)} AND {m.group(3)};
                """).strip()
            ),
            
            # TEMPLATE 10: COUNT+BETWEEN
            # Counts rows where a column value is between two values
            
            # Count rows where column_name value is between 1 and 3 in table_name
            # Count rows where CreditHours value is between 5 and 7 in Courses
            (
                r"(?:count)?\s*rows where (.+) value is between (.+) and (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*)
                    FROM {m.group(4)}
                    WHERE {m.group(1)} BETWEEN {m.group(2)} AND {m.group(3)};
                """).strip()
            ),
            
            
            
            # TEMPLATE 11: !=
            # Lists rows where a column is not equal to a value
            
            # List rows where column_name is not equal to '...' in table_name
            # List rows where CreditHours is not equal to 5 in Courses
            (
                r"(?:find|list)?\s*rows where (.+) is not equal to (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} != {m.group(2)};
                """).strip()
            ),
            
            # TEMPLATE 12: COUNT+!=
            # Counts rows where a column is not equal to a value
            
            # Count rows where column_name is not equal to '...' in table_name
            # Count rows where CreditHours is not equal to 5 in Courses
            (
                r"(?:Count)?\s*rows where (.+) is not equal to (.+) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*)
                    FROM {m.group(3)}
                    WHERE {m.group(1)} != {m.group(2)};
                """).strip()
            ),
            

            # TEMPLATE 13: LIKE (contains)
            # Lists rows where a column contains a specific value
            
            # List rows where column_name contains '...' in table_name
            # List rows where FirstName contains 'Maria' in Students
            
            (
                r"(?:find|list)?\s*rows where (.+) contains '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '%{m.group(2)}%';
                """).strip()
            ),
            
            
            
            
            # TEMPLATE 14: COUNT+LIKE (contains)
            # Counts rows where a column contains a specific value

            # Count rows where column_name contains '...' in table_name
            # Count rows where FirstName contains 'Maria' in Students
            
            (
                r"(?:count)?\s*rows where (.+) contains '(.+)' in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) 
                    FROM {m.group(3)}
                    WHERE {m.group(1)} LIKE '%{m.group(2)}%';
                """).strip()
            ),
            
            
            # TEMPLATE 15:
            # Lists the first N rows in a table
            # List first 10 rows in Courses
            (
                r"(?:find|list)?\s*first (\d+) rows in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(2)}
                    LIMIT {m.group(1)};
                """).strip()
            ),
            
            # TEMPLATE 16: IS NULL
            # Lists rows where a column is null
            # List rows where CreditHours is null in Courses
            (
                r"(?:find|list)?\s*rows where (.+) is null in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(2)}
                    WHERE {m.group(1)} IS NULL;
                """).strip()
            ),
            
            # TEMPLATE 17: IS NOT NULL
            # Lists rows where a column is not null
            # List rows where CreditHours is not null in Courses
            (
                r"(?:find|list)?\s*rows where (.+) is not null in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(2)}
                    WHERE {m.group(1)} IS NOT NULL;
                """).strip()
            ),
            
            # TEMPLATE 18: COUNT+IS NULL
            # Count rows where a column is null
            (
                r"(?:find|count)?\s*rows where (.+) is null in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) 
                    FROM {m.group(2)}
                    WHERE {m.group(1)} IS NULL;
                """).strip()
                
            ),
            
            # TEMPLATE 19: COUNT+IS NOT NULL
            # Counts rows where a column is not null
            (
                r"(?:find|count)?\s*rows where (.+) is not null in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*) 
                    FROM {m.group(2)}
                    WHERE {m.group(1)} IS NOT NULL;
                """).strip()
            ),
        ]
        
        
        # Count rows where CreditHours is not null in Courses
        
        
        
        query_templates += [
            
            # TEMPLATE 20: IN
            # Finds rows where a column has any value in a list
            (
                r"(?:find|list)?\s*rows where (.+) has any value in \((.+)\) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT *
                    FROM {m.group(3)}
                    WHERE {m.group(1)} IN ({m.group(2)});
                """).strip()
            ),
            
            # TEMPLATE 21: COUNT+IN
            # Counts rows where a column has any value in a list
            (
                r"(?:find|count)?\s*rows where (.+) has any value in \((.+)\) in (.+)",
                lambda m: textwrap.dedent(f"""
                    SELECT COUNT(*)
                    FROM {m.group(3)}
                    WHERE {m.group(1)} IN ({m.group(2)});
                """).strip()
            ),
        ]



        # Function to match and generate queries
        def generate_query(user_query, templates):
            for pattern, query_func in templates:
                match = re.match(pattern, user_query, re.IGNORECASE)
                if match:
                    return query_func(match)
            return None


        # Generate and print SQL queries for test queries
        for user_query in test_queries:
            sql_query = generate_query(user_query, query_templates)
            if sql_query:
                #print(f"User Query: {user_query}")
                #print(f"Generated SQL Query:\n{sql_query}\n")
                # print(sql_query)
                
                return sql_query
            #else:
                #print(f"No match found for query: {user_query}")
                



# Test schema dictionary
schema_dict = {
    "courses": ["CourseID", "CourseName", "InstructorID", "InstructorName", "CreditHours"],
    "enrollments": ["EnrollmentID", "StudentID", "CourseID", "Semester", "Grade"],
    "students": ["StudentID", "FirstName", "LastName", "Email", "Major", "AdvisorID", "AdvisorName"]
}

# # Test cases to try different scenarios
# test_queries = [
#     # Single table query
#     "FirstName LastName Major",  # Only from students table
    
#     # Multi-table query requiring join
#     "FirstName Grade CourseID",  # From students, enrollments tables
    
#     # Multi-table query requiring multiple joins 
#     "FirstName CourseName Grade", # From students, courses, enrollments tables
    
#     # Invalid column names
#     "InvalidColumn FirstName",  # Contains invalid column
# ]

# # Run test cases
# for query in test_queries:
#     print(f"\nQuery: {query}")
#     result = check_join_needed(query, schema_dict)
#     print(f"Join needed: {result}")

def sql_to_mongo(sql_query):
    """
    Convert SQL queries to MongoDB aggregation pipelines.
    """
    sql_query = sql_query.strip()
    
    # 1
    # Handle SELECT * FROM table_name LIMIT 5
    match_limit = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)\s+LIMIT\s+(\d+)", sql_query, re.IGNORECASE)
    if match_limit:
        collection, limit = match_limit.groups()
        return f"db.{collection}.aggregate([{{ '$limit': {int(limit)} }}]);"

    # 2, 6
    # Handle SELECT * FROM table_name ORDER BY column_name ASC|DESC LIMIT 5
    match_order_limit = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+ORDER\s+BY\s+(\w+)\s*(ASC|DESC)?\s+LIMIT\s+(\d+)", 
        sql_query, re.IGNORECASE
    )
    if match_order_limit:
        collection, column_name, order, limit = match_order_limit.groups()
        sort_order = 1 if order and order.upper() == "ASC" else -1
        return f"db.{collection}.aggregate([{{ '$sort': {{ '{column_name}': {sort_order} }} }}, {{ '$limit': {int(limit)} }}]);"
    
    # 3
    # Handle SELECT COUNT(*) FROM table_name
    match_count_all = re.match(r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)", sql_query, re.IGNORECASE)
    if match_count_all:
        collection = match_count_all.group(1)
        return f"db.{collection}.aggregate([{{ '$count': 'total_count' }}]);"
    

    #4
    # Handle SELECT * FROM table_name WHERE column_name BETWEEN 1 AND 3
    match_between = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)", sql_query, re.IGNORECASE)
    if match_between:
        collection, column_name, start, end = match_between.groups()
        return (
            f"db.{collection}.aggregate([{{ '$match': {{ '{column_name}': {{ '$gte': {int(start)}, '$lte': {int(end)} }} }} }}]);"
        )
    
    # 5
    # Handle SELECT * FROM table_name WHERE column_name != '...'
    match_not_equal = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+!=\s+(.+?)", sql_query, re.IGNORECASE)
    if match_not_equal:
        collection, column_name, value = match_not_equal.groups()
        return f"db.{collection}.aggregate([{{ '$match': {{ '{column_name}': {{ '$ne': {value} }} }} }}]);"
    
    # 7
    # Handle SELECT * FROM table_name WHERE column_name IS NULL
    match_is_null = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IS\s+NULL", sql_query, re.IGNORECASE)
    if match_is_null:
        collection, column_name = match_is_null.groups()
        return f"db.{collection}.aggregate([{{ '$match': {{ '{column_name}': {{ '$exists': true, '$eq': null }} }} }}]);"

    # 8
    # Handle SELECT * FROM table_name WHERE column_name IS NOT NULL
    match_is_not_null = re.match(r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IS\s+NOT\s+NULL", sql_query, re.IGNORECASE)
    if match_is_not_null:
        collection, column_name = match_is_not_null.groups()
        return f"db.{collection}.aggregate([{{ '$match': {{ '{column_name}': {{ '$exists': true, '$ne': null }} }} }}]);"
    
    
    # 1 - 8
    
    ####################
    
    
    # 9
    # Handle SELECT * FROM table_name WHERE column_name LIKE '%pattern%'
    # Handle SELECT * FROM table_name WHERE column_name LIKE '%pattern%'
    # Handle SELECT * FROM table_name WHERE column_name IN (value1, value2, value3)
    # Handle SELECT * FROM table_name WHERE column_name IN (value1, value2, value3)
    match_in = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IN\s+\((.+)\)", 
        sql_query.strip(), 
        re.IGNORECASE
    )
    if match_in:
        collection, column_name, values = match_in.groups()
        value_list = [v.strip() for v in values.split(",")]  # Split values and trim whitespace
        # Convert numeric values to integers if possible
        value_list = [int(v) if v.isdigit() else v.strip("'\"") for v in value_list]
        return f"db.{collection}.aggregate([{{ '$match': {{ '{column_name}': {{ '$in': {value_list} }} }} }}]);"
    
    # 10
    # Handle SELECT column_name, COUNT(aggregate_column) FROM table_name GROUP BY group_column
    match_group_by = re.match(
        r"SELECT\s+(\w+)\s*,\s*COUNT\((\w+)\)\s+FROM\s+(\w+)\s+GROUP\s+BY\s+(\w+)", 
        sql_query.strip(), 
        re.IGNORECASE
    )
    if match_group_by:
        group_column, aggregate_column, collection, group_by_column = match_group_by.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': '${group_column}', 'count': {{ '$sum': 1 }} }} }},"
            f"{{ '$project': {{ '{group_column}': '$_id', '_id': 0, 'count': 1 }} }}"
            f"]);"
        )

    # 11
    # Handle SELECT column_name, AVG(aggregate_column) FROM table_name GROUP BY group_column
    match_group_avg = re.match(
        r"SELECT\s+(\w+)\s*,\s*AVG\((\w+)\)\s+FROM\s+(\w+)\s+GROUP\s+BY\s+(\w+)", 
        sql_query.strip(), 
        re.IGNORECASE
    )
    if match_group_avg:
        group_column, aggregate_column, collection, group_by_column = match_group_avg.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': '${group_column}', 'avg_salary': {{ '$avg': '${aggregate_column}' }} }} }},"
            f"{{ '$project': {{ '{group_column}': '$_id', '_id': 0, 'avg_salary': 1 }} }}"
            f"]);"
        )

    
    
    # TEMPLATES FOR JOINS
    
    # 1
    
    # Handle SELECT with JOIN
    match_join = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s*=\s*'(.+)'",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field, filter_value = match_join.groups()
        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{join_field1.split('.')[1]}', 'foreignField': '{join_field2.split('.')[1]}', 'as': 'joined_docs' }} }},"
            f"{{ '$unwind': '$joined_docs' }},"
            f"{{ '$match': {{ '{filter_field}': '{filter_value}' }} }},"
            f"{{ '$project': {{ '{select_field1.split('.')[1]}': 1, '{select_field2.split('.')[1]}': '$joined_docs.{select_field2.split('.')[1]}', '_id': 0 }} }}"
            f"]);"
        )

    # 3
    # Handle SELECT with JOIN and IS NULL condition
    match_join_is_null = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s+IS\s+NULL",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join_is_null:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field = match_join_is_null.groups()
        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{join_field1.split('.')[1]}', 'foreignField': '{join_field2.split('.')[1]}', 'as': 'joined_docs' }} }},"
            f"{{ '$unwind': '$joined_docs' }},"
            f"{{ '$match': {{ '{filter_field}': {{ '$exists': true, '$eq': null }} }} }},"
            f"{{ '$project': {{ '{select_field1.split('.')[1]}': 1, '{select_field2.split('.')[1]}': '$joined_docs.{select_field2.split('.')[1]}', '_id': 0 }} }}"
            f"]);"
        )

    #4
    # Handle SELECT with JOIN and IS NOT NULL condition
    match_join_is_not_null = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s+IS\s+NOT\s+NULL",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join_is_not_null:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field = match_join_is_not_null.groups()
        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{join_field1.split('.')[1]}', 'foreignField': '{join_field2.split('.')[1]}', 'as': 'joined_docs' }} }},"
            f"{{ '$unwind': '$joined_docs' }},"
            f"{{ '$match': {{ '{filter_field}': {{ '$exists': true, '$ne': null }} }} }},"
            f"{{ '$project': {{ '{select_field1.split('.')[1]}': 1, '{select_field2.split('.')[1]}': '$joined_docs.{select_field2.split('.')[1]}', '_id': 0 }} }}"
            f"]);"
        )
    
    ########
    
    #5
    # Handle SELECT with JOIN and IN condition
    match_join_in = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s+IN\s+\((.+)\)",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join_in:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field, filter_values = match_join_in.groups()
        value_list = [v.strip().strip("'\"") for v in filter_values.split(",")]  # Split and clean values
        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{join_field1.split('.')[1]}', 'foreignField': '{join_field2.split('.')[1]}', 'as': 'joined_docs' }} }},"
            f"{{ '$unwind': '$joined_docs' }},"
            f"{{ '$match': {{ '{filter_field}': {{ '$in': {value_list} }} }} }},"
            f"{{ '$project': {{ '{select_field1.split('.')[1]}': 1, '{select_field2.split('.')[1]}': '$joined_docs.{select_field2.split('.')[1]}', '_id': 0 }} }}"
            f"]);"
        )
    
    #6
    # Handle SELECT with JOIN and NOT IN condition
    match_join_not_in = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s+NOT\s+IN\s+\((.+)\)",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join_not_in:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field, filter_values = match_join_not_in.groups()
        value_list = [v.strip().strip("'\"") for v in filter_values.split(",")]  # Split and clean values
        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{join_field1.split('.')[1]}', 'foreignField': '{join_field2.split('.')[1]}', 'as': 'joined_docs' }} }},"
            f"{{ '$unwind': '$joined_docs' }},"
            f"{{ '$match': {{ '{filter_field}': {{ '$nin': {value_list} }} }} }},"
            f"{{ '$project': {{ '{select_field1.split('.')[1]}': 1, '{select_field2.split('.')[1]}': '$joined_docs.{select_field2.split('.')[1]}', '_id': 0 }} }}"
            f"]);"
        )




        # Raise error for unsupported queries
    print(f"Unsupported query format: {sql_query}")
    raise ValueError("Unsupported query format.")



    
# query_test="get Grade, Major where Grade = 'A'"
# print(query_function_sql(data_schema=schema_dict,query=query_test))

# print(sql_to_mongo(query_function_sql(data_schema=schema_dict,query=query_test)))