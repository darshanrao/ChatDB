
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
                r"(?:find|list)?\s*rows where (.+) value is between (.+) and (.+) in (.+)",
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
            # import pdb; pdb.set_trace()
            
            if sql_query:
                #print(f"User Query: {user_query}")
                #print(f"Generated SQL Query:\n{sql_query}\n")
                sql_query = sql_query.replace('\n', ' ')
                # print(sql_query)
                # import pdb; pdb.set_trace()
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
        sort_order = 1 
        return f"db.{collection}.aggregate([{{ '$sort': {{ '{column_name}': {sort_order} }} }}, {{ '$limit': {int(limit)} }}]);"
    
    # 3
    # Handle SELECT COUNT(*) FROM table_name
    match_count_all = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+);?$",  # Ensure the query ends after the table name
        sql_query,
        re.IGNORECASE
    )
    if match_count_all:
        collection = match_count_all.group(1)
        # return f"db.{collection}.countDocuments({{}});"
        return f"db.{collection}.aggregate([{{ $count: 'count' }}]);"

    

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
        return f"db.{collection}.aggregate([{{ '$match': {{ {column_name}: {{ '$ne': {value} }} }} }}]);"
    
    
    # 6
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name BETWEEN value1 AND value2
    
    match_count_between = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+);?$",
        sql_query, 
        re.IGNORECASE
    )
    if match_count_between:
        collection, column_name, start, end = match_count_between.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$gte': {int(start)}, '$lte': {int(end)} }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )
    
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
    
    
    
    # 9
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
    # Handle SELECT with WHERE clause and LIKE operator
    match_like = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'%(.+)%'",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_like:
        
        #print("%match%")
        
        collection, field, pattern = match_like.groups()

        # Convert SQL LIKE pattern to MongoDB regex (only handle prefix match here)
        mongo_regex = pattern.rstrip('%')  # Remove trailing '%' for starts-with regex

        return (
            # f"db.{collection}.find({{ {field}: {{ $regex: \".*{mongo_regex}.*\" }} }});"
            f"db.{collection}.aggregate([{{ $match: {{ {field}: {{ $regex: \"^{mongo_regex}\" }} }} }}]);"
            )
    
    # 11
    
    # Handle SELECT with WHERE clause and greater-than condition
    match_greater_than = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*>\s*(\d+)",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_greater_than:
        collection, field, value = match_greater_than.groups()

        # Convert to MongoDB query
        return (
            # f"db.{collection}.find({{ {field}: {{ $gt: {value} }} }});"
            f"db.{collection}.aggregate([{{ $match: {{ {field}: {{ $gt: {value} }} }} }}]);"
        )
    
    
    # 12
    
    # Handle SELECT with WHERE clause and less-than condition
    match_less_than = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*<\s*(\d+)",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_less_than:
        collection, field, value = match_less_than.groups()

        # Convert to MongoDB query
        return (
            # f"db.{collection}.find({{ {field}: {{ $lt: {value} }} }});"
            f"db.{collection}.aggregate([{{ $match: {{ {field}: {{ $lt: {value} }} }} }}]);"
        )
        
    # 13
    # Handle SELECT COUNT(DISTINCT column_name) AS alias FROM table_name
    match_count_distinct = re.match(
        r"SELECT\s+COUNT\(DISTINCT\s+(\w+)\)\s+AS\s+(\w+)\s+FROM\s+(\w+);?",
        sql_query, re.IGNORECASE
    )
    if match_count_distinct:
        column_name, alias, collection = match_count_distinct.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': '${column_name}' }} }},"
            f"{{ '$count': '{alias}' }}"
            f"]);"
        )
    
    
    # 14
    
    # Handle SELECT SUM(column_name) AS alias FROM table_name
    match_sum = re.match(
        r"SELECT\s+SUM\((\w+)\)\s+AS\s+(\w+)\s+FROM\s+(\w+);?",
        sql_query, re.IGNORECASE
    )
    if match_sum:
        column_name, alias, collection = match_sum.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': null, '{alias}': {{ '$sum': '${column_name}' }} }} }}"
            f"]);"
        )

    
    # 15
    # Handle SELECT AVG(column_name) AS alias FROM table_name
    match_avg = re.match(
        r"SELECT\s+AVG\((\w+)\)\s+AS\s+(\w+)\s+FROM\s+(\w+);?",
        sql_query, re.IGNORECASE
    )
    if match_avg:
        column_name, alias, collection = match_avg.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': null, '{alias}': {{ '$avg': '${column_name}' }} }} }}"
            f"]);"
        )
    
    
    # 16
    # Handle SELECT MIN(column_name) AS alias FROM table_name
    match_min = re.match(
        r"SELECT\s+MIN\((\w+)\)\s+AS\s+(\w+)\s+FROM\s+(\w+);?",
        sql_query, re.IGNORECASE
    )
    if match_min:
        column_name, alias, collection = match_min.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': null, '{alias}': {{ '$min': '${column_name}' }} }} }}"
            f"]);"
        )
    
    
    # 17
    # Handle SELECT MAX(column_name) AS alias FROM table_name
    match_max = re.match(
        r"SELECT\s+MAX\((\w+)\)\s+AS\s+(\w+)\s+FROM\s+(\w+);?",
        sql_query, re.IGNORECASE
    )
    if match_max:
        column_name, alias, collection = match_max.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': null, '{alias}': {{ '$max': '${column_name}' }} }} }}"
            f"]);"
        )
    
    # 18
    # Handle SELECT with WHERE clause and LIKE operator
    match_like = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'%(.+)'",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_like:
        
        #print("%match")
        
        collection, field, pattern = match_like.groups()

        # Convert SQL LIKE pattern to MongoDB regex (only handle prefix match here)
        mongo_regex = pattern.rstrip('%')  # Remove trailing '%' for starts-with regex

        return (
            # f"db.{collection}.find({{ {field}: {{ $regex: \"{mongo_regex}$\" }} }});"
            f"db.{collection}.aggregate([{{ $match: {{ {field}: {{ $regex: \"{mongo_regex}$\" }} }} }}]);"
        )
    
    
    # 19
    # Handle SELECT with WHERE clause and LIKE operator
    match_like = re.match(
        r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'(.+)%'",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_like:
        
        #print("match%")
        
        collection, field, pattern = match_like.groups()

        # Convert SQL LIKE pattern to MongoDB regex (only handle prefix match here)
        mongo_regex = pattern.rstrip('%')  # Remove trailing '%' for starts-with regex

        return (
            # f"db.{collection}.find({{ {field}: {{ $regex: \"^{mongo_regex}\" }} }});")
            f"db.{collection}.aggregate([{{ $match: {{ {field}: {{ $regex: \"^{mongo_regex}\" }} }} }}]);")
    
    
    # 20
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name LIKE '%value%'
    match_count_like = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'%(.+?)%';?",
        sql_query, re.IGNORECASE
    )
    if match_count_like:
        collection, column_name, regex_pattern = match_count_like.groups()
        # Convert SQL LIKE pattern to MongoDB regex
        #regex_pattern = pattern.replace('%', '.*')
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$regex': '.*{regex_pattern}.*' }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )
    
    
    # 21
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name LIKE '%value'
    match_count_like = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'%(.+?)';?",
        sql_query, re.IGNORECASE
    )
    if match_count_like:
        collection, column_name, regex_pattern = match_count_like.groups()
        # Convert SQL LIKE pattern to MongoDB regex
        #regex_pattern = pattern.replace('%', '.*')
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$regex': '{regex_pattern}$' }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )

    
    # 22
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name LIKE '%value'
    match_count_like = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+LIKE\s+'(.+?)%';?",
        sql_query, re.IGNORECASE
    )
    if match_count_like:
        collection, column_name, regex_pattern = match_count_like.groups()
        # Convert SQL LIKE pattern to MongoDB regex
        #regex_pattern = pattern.replace('%', '.*')
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$regex': '^{regex_pattern}' }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )
    
    # 23
    # Handle SELECT MAX(column_name) - MIN(column_name) FROM table_name
    match_max_min_diff = re.match(
        r"SELECT\s+MAX\((\w+)\)\s*-\s*MIN\(\1\)\s+FROM\s+(\w+);?$",
        sql_query, 
        re.IGNORECASE
    )
    if match_max_min_diff:
        column_name, collection = match_max_min_diff.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$group': {{ '_id': null, 'max_value': {{ '$max': '${column_name}' }}, 'min_value': {{ '$min': '${column_name}' }} }} }},"
            f"{{ '$project': {{ '_id': 0, 'difference': {{ '$subtract': [ '$max_value', '$min_value' ] }} }} }}"
            f"]);"
        )

    
    # 24 
    
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name IS NULL
    match_count_is_null = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IS\s+NULL;?$",
        sql_query, 
        re.IGNORECASE
    )
    if match_count_is_null:
        collection, column_name = match_count_is_null.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$eq': null }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )
    
    # 25 
    # Handle SELECT COUNT(*) FROM table_name WHERE column_name IS NULL
    match_count_is_null = re.match(
        r"SELECT\s+COUNT\(\*\)\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IS\s+NOT NULL;?$",
        sql_query, 
        re.IGNORECASE
    )
    if match_count_is_null:
        collection, column_name = match_count_is_null.groups()
        return (
            f"db.{collection}.aggregate(["
            f"{{ '$match': {{ '{column_name}': {{ '$exists': true, '$ne': null }} }} }},"
            f"{{ '$count': 'total_count' }}"
            f"]);"
        )
    
    # TEMPLATES FOR JOINS
    
    # 1
    
   # Template to handle SELECT with JOIN and WHERE clause
    match_join = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s*=\s*('?)(.+?)\8",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field, _, filter_value = match_join.groups()

        # Convert numeric filter values to integers if applicable
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = f'"{filter_value}"'  # Use double quotes for strings

        # Handle splitting fields
        local_field = join_field1.split('.')[1] if '.' in join_field1 else join_field1
        foreign_field = join_field2.split('.')[1] if '.' in join_field2 else join_field2
        filter_field_name = filter_field.split('.')[1] if '.' in filter_field else filter_field
        select_field1_name = select_field1.split('.')[1] if '.' in select_field1 else select_field1
        select_field2_name = select_field2.split('.')[1] if '.' in select_field2 else select_field2

        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{local_field}', 'foreignField': '{foreign_field}', 'as': '{collection2}' }} }},"
            f"{{ '$unwind': '${collection2}' }},"
            f"{{ '$match': {{ '{filter_field_name}': {filter_value} }} }},"
            f"{{ '$project': {{ '{select_field1_name}': 1, '{select_field2_name}': '${collection2}.{select_field2_name}' }} }}"
            f"]);"
        )
        
    #2
    # Handle SELECT with JOIN and WHERE IN
    match_join_in = re.match(
        r"SELECT\s+([\w.,\s]+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s+WHERE\s+(\w+)\s+IN\s+\((.+?)\);?",
        sql_query,
        re.IGNORECASE
    )
    if match_join_in:
        select_fields, collection1, collection2, table1, key1, table2, key2, filter_column, in_values = match_join_in.groups()
        
        # Parse fields for projection
        fields = [field.strip() for field in select_fields.split(",")]
        projection = {field.split(".")[1]: f"${field.split('.')[1]}" for field in fields}

        # Parse values for the IN clause
        in_values = [value.strip().strip("'") for value in in_values.split(",")]

        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{key1}', 'foreignField': '{key2}', 'as': 'joined_data' }} }},"
            f"{{ '$unwind': '$joined_data' }},"
            f"{{ '$match': {{ '{filter_column}': {{ '$in': {in_values} }} }} }},"
            f"{{ '$project': {projection} }}"
            f"]);"
        )
    
    
    
    # 3
    # Template to handle SELECT with JOIN and WHERE clause using "not equal to" condition
    match_join_not_equal = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s*!=\s*('?)(.+?)\8",
        sql_query.strip(),
        re.IGNORECASE
    )
    if match_join_not_equal:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field, _, filter_value = match_join_not_equal.groups()

        # Convert numeric filter values to integers if applicable
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = f'"{filter_value}"'  # Use double quotes for strings

        # Handle splitting fields
        local_field = join_field1.split('.')[1] if '.' in join_field1 else join_field1
        foreign_field = join_field2.split('.')[1] if '.' in join_field2 else join_field2
        filter_field_name = filter_field.split('.')[1] if '.' in filter_field else filter_field
        select_field1_name = select_field1.split('.')[1] if '.' in select_field1 else select_field1
        select_field2_name = select_field2.split('.')[1] if '.' in select_field2 else select_field2

        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{local_field}', 'foreignField': '{foreign_field}', 'as': '{collection2}' }} }},"
            f"{{ '$unwind': '${collection2}' }},"
            f"{{ '$match': {{ '{filter_field_name}': {{ '$ne': {filter_value} }} }} }},"
            f"{{ '$project': {{ '{select_field1_name}': 1, '{select_field2_name}': '${collection2}.{select_field2_name}' }} }}"
            f"]);"
        )
    
    #4
    
     # Handle SELECT with JOIN and multiple WHERE conditions
    match_join_where = re.match(
        r"SELECT\s+(\w+\.\w+),\s+(\w+\.\w+)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+WHERE\s+(\w+)\s*>\s*(\d+)\s+AND\s+(\w+)\s+LIKE\s+'(.+?)';?",
        sql_query,
        re.IGNORECASE
    )
    if match_join_where:
        select_field1, select_field2, collection1, collection2, join_field1, join_field2, filter_field1, filter_value1, filter_field2, filter_value2 = match_join_where.groups()

        # Convert LIKE pattern to MongoDB regex
        regex_pattern = filter_value2.replace('%', '.*')

        # Handle splitting fields
        local_field = join_field1.split('.')[1] if '.' in join_field1 else join_field1
        foreign_field = join_field2.split('.')[1] if '.' in join_field2 else join_field2
        filter_field1_name = filter_field1.split('.')[1] if '.' in filter_field1 else filter_field1
        filter_field2_name = filter_field2.split('.')[1] if '.' in filter_field2 else filter_field2
        select_field1_name = select_field1.split('.')[1] if '.' in select_field1 else select_field1
        select_field2_name = select_field2.split('.')[1] if '.' in select_field2 else select_field2

        return (
            f"db.{collection1}.aggregate(["
            f"{{ '$lookup': {{ 'from': '{collection2}', 'localField': '{local_field}', 'foreignField': '{foreign_field}', 'as': '{collection2}' }} }},"
            f"{{ '$unwind': '${collection2}' }},"
            f"{{ '$match': {{ "
            f"    '{filter_field1_name}': {{ '$gt': {int(filter_value1)} }}, "
            f"    '{collection2}.{filter_field2_name}': {{ '$regex': '{regex_pattern}', '$options': 'i' }} "
            f"}} }},"
            f"{{ '$project': {{ '{select_field1_name}': 1, '{select_field2_name}': '${collection2}.{select_field2_name}' }} }}"
            f"]);"
        )


    # Raise error if no match
    print(f"Unsupported query format: {sql_query.strip()}")
    raise ValueError("Unsupported query format.")

    
# query_test="List top 5 rows in students"
# print(query_function_sql(data_schema=schema_dict,query=query_test))

# print(sql_to_mongo(query_function_sql(data_schema=schema_dict,query=query_test)))