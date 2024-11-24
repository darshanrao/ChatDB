import google.generativeai as genai
import json
API_KEY="AIzaSyB1LLI9pNfaU1wVmW39j1zbw1WXHxHVkKM"
class Decomposer:
    def __init__(self,model_name: str = "gemini-1.5-flash"):
        """
        
        Initializes the Gemini model handler with a base prompt.
        """

        self.content = """
        ### Task:
            Decompose the given query into multiple independent queries.
            Please respond only in JSON format with the keys "time" and "queries".

            ### Instructions:
            - The time should be in the form of "mm:ss".
            - If the time is not mentioned in the prompt set it to default of "01:00".
            - Only generate queries from the information provided in the user prompt.
            - The query generated should have less ambiguity.
            - Use everyday language to refine your query.

            ### Example 1:
            User Prompt: Describe a 2-minute video featuring Serena Williams showcasing her training routine, including a comparison with her competitors and discussing how her unique style impacts her performance.

            Decomposed Queries:
            {
                "time": "2:00",
                "queries": [
                    "Highlight Serena Williams' training routine",
                    "Compare Serena Williams' training routine to her competitors",
                    "Discuss how Serena Williams' unique style impacts her performance",
                    ]
            }

            ### Example 2:
            User Prompt: Create a detailed 90-second video about Tesla's Autopilot technology, explaining how it works, discussing safety concerns, and comparing it to other autonomous systems on the market.

            Decomposed Queries:
            {
                "time": "1:30",
                "queries": [
                    "Working of Tesla's Autopilot technology",
                    "Safety concerns surrounding Tesla's Autopilot",
                    "Tesla's Autopilot technology to other autonomous systems on the market",
                    ]
            }

            ### Example 3:
            User Prompt: Generate a 1-minute video showcasing a day in the life of an astronaut on the International Space Station, including their work routine, leisure activities, and how they adapt to zero gravity.

            Decomposed Queries:
            {
                "time": "1:00",
                "queries": [
                    "The work routine of an astronaut on the International Space Station",
                    "The leisure activities of an astronaut on the International Space Station",
                    "Astronauts adapting to zero gravity",
                    ]
            }

            ### Example 4:
            User Prompt: <input>

            Decomposed Queries:
        """
        self.model_name = model_name

        # Configure generative.ai with your API key (replace with yours)
        genai.configure(api_key=API_KEY)
        # Load the Gemini model using generative.ai
        self.model = genai.GenerativeModel(model_name=self.model_name)

    def decompose(self, input_prompt: str) -> str:
        """Send a prompt to the Gemini model and retrieve the output as JSON."""
        # Add input prompt to the full prompt
        full_prompt = self.content.replace('<input>', input_prompt)
        time = ''
        query_list=[]
        retries = 5

        while len(query_list) == 0 and retries:
            retries -= 1

            response = self.model.generate_content(full_prompt)

            # Access the generated text
            sequence = response.text
            
            start_idx, end_idx = sequence.find('{'), sequence.find('}')
            if start_idx != -1 and end_idx != -1:
                json_str = sequence[start_idx:end_idx + 1]
                try:
                    data = json.loads(json_str)
                    query_list = data["queries"]
                    time = data["time"]
                except json.JSONDecodeError:
                    query_list = []
                    time = ''
            else:
                continue


        return convert_time_to_seconds(time), query_list
        

def convert_time_to_seconds(time_str: str) -> int:


    # Split the string by the colon
    minutes, seconds = time_str.split(":")
    
    # Convert to integers and calculate total seconds
    total_seconds = int(minutes) * 60 + int(seconds)
    
    return total_seconds


# Create an instance of the GeminiModelHandler class
gemini_handler = Decomposer()

# Define a sample input prompt
sample_prompt = "Focus a 90-second video about Turkish Sharpshooter Yusuf Dikec, describing his casual and unorthodox style and approach to the sport. Compare and contrast him to the more traditional competitors in this event. Make references to his backstory, including the potential that he might be a hitman."

# Call the decomposer method with the sample prompt
time, query_list = gemini_handler.decompose(sample_prompt)

# Print the results
print("Time: ", time)
print("Query List: ", query_list)

# Verify the output for debugging
if query_list:
    print("Successfully parsed queries:", query_list)
else:
    print("Failed to parse queries. Please check the output format.")