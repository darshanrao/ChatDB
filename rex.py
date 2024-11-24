import google.generativeai as genai
import json
import os
from copy import deepcopy
class QueryER:
    def __init__(self,model_name: str = "gemini-1.5-flash"):

        self.content = """
        You are is SQL expert and you have to write sql query for the given prompt and the dataschema
            ### Task:
            - Use the query and schema to write a SQL query
            - Please respond only in JSON format with the keys "sql" with mySQL query as value 

            ### Instructions:
            - Try to use JOINs in the SQL query rather than Subquerying


            ### Example 1:
            User Prompt: find Grade, CourseName where grade > 90 and courseName like '%Math%'

            DataBase Schema:
            "tables": {
                "courses": ["CourseID","CourseName","InstructorID","InstructorName","CreditHours"],
                "enrollments": ["EnrollmentID","StudentID","CourseID","Semester","Grade"],
                "students": ["StudentID","FirstName","LastName","Email","Major","AdvisorID","AdvisorName"]
            }

            MySql Query:
            {
            "sql": "SELECT credithours, MAX(credithours) AS max_value
                FROM courses
                GROUP BY credithours
                ORDER BY max_value DESC
                LIMIT 1;",
            }

            ### Example 2:
            User Prompt: list Grade, CourseName in context where grade between 50 and 90

            DataBase Schema:
            "tables": {
                "courses": ["CourseID","CourseName","InstructorID","InstructorName","CreditHours"],
                "enrollments": ["EnrollmentID","StudentID","CourseID","Semester","Grade"],
                "students": ["StudentID","FirstName","LastName","Email","Major","AdvisorID","AdvisorName"]
            }

            MySql Query:
            {
            "sql": "SELECT enrollments.Grade, courses.CourseName
                FROM courses
                JOIN enrollments ON courses.CourseID = enrollments.CourseID
                WHERE grade BETWEEN 50 AND 90;",
            }


            ### Example 3:
            User Prompt: get Grade, Major where Grade is not null

            DataBase Schema:
            "tables": {
                "courses": ["CourseID","CourseName","InstructorID","InstructorName","CreditHours"],
                "enrollments": ["EnrollmentID","StudentID","CourseID","Semester","Grade"],
                "students": ["StudentID","FirstName","LastName","Email","Major","AdvisorID","AdvisorName"]
            }

            MySql Query:
            {
            "sql": "SELECT enrollments.Grade, courses.CourseName
                FROM courses
                JOIN enrollments ON courses.CourseID = enrollments.CourseID
                WHERE grade BETWEEN 50 AND 90;",
            }

            ### Example 4:
            User Prompt: <input>

            DataBase Schema:
            <schema>


            MySql Query:
        """
        self.model_name = model_name

        # Configure generative.ai with your API key (replace with yours)
        genai.configure(api_key=os.getenv("API_KEY"))
        # Load the Gemini model using generative.ai
        self.model = genai.GenerativeModel(model_name=self.model_name)

    def decompose(self, input_prompt: str, dataschema ) -> str:

        full_prompt = deepcopy(self.content)
        full_prompt = full_prompt.replace('<input>', input_prompt)
        full_prompt = full_prompt.replace('<schema>', dataschema)
        query=''
        retries = 5

        while len(query) == 0 and retries:
            retries -= 1

            response = self.model.generate_content(full_prompt)
            # import pdb; pdb.set_trace()
            # Access the generated text
            sequence = response.text
            
            start_idx, end_idx = sequence.find('{'), sequence.find('}')
            if start_idx != -1 and end_idx != -1:
                json_str = sequence[start_idx:end_idx + 1]
                try:
                    data = json.loads(json_str)
                    query = data["sql"]
                except json.JSONDecodeError:
                    query=''

            else:
                continue


        return query
        



# Create an instance of the GeminiModelHandler class
gemini_handler = QueryER()

# Define a sample input prompt
sample_query_prompt = "what are the Grade, CourseName with the the the grade = 100"
schema= """ "tables": {
                "courses": ["CourseID","CourseName","InstructorID","InstructorName","CreditHours"],
                "enrollments": ["EnrollmentID","StudentID","CourseID","Semester","Grade"],
                "students": ["StudentID","FirstName","LastName","Email","Major","AdvisorID","AdvisorName"]
            }
            """
# Call the decomposer method with the sample prompt
query = gemini_handler.decompose(sample_query_prompt,schema)

# Print the results
print("Query List: ", query)

# Verify the output for debugging
if query:
    print("Successfully parsed queries:", query)
else:
    print("Failed to parse queries. Please check the output format.")