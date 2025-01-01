# ChatDB AI

![image](https://github.com/user-attachments/assets/08cf653f-f1f7-4f57-be80-105f403ace28)

ChatDB is an intuitive application designed to simplify working with relational data. It allows users to:  

- Upload relational data in CSV format to **MongoDB** (stored as collections) or **Amazon RDS**.  
- Query the uploaded data using **human-like queries** that are translated into corresponding SQL or MongoDB queries by the backend.
- Uses Gemini 1.5 Flash LLM  to generate the queries  
- View query results in a tabular format on the dashboard.  

---

## Features

1. **Upload Relational Data**  
   - Upload a folder containing CSV files.  
   - The folder name is treated as the database name.  
   - CSV files within the folder are saved as collections (in MongoDB) or tables (in Amazon RDS).  

2. **Query Execution**  
   - Write queries in a natural language format (e.g., *"Get all customers from California"*).  
   - The backend converts these queries into either:  
     - SQL queries for Amazon RDS.  
     - MongoDB queries for MongoDB.  
   - Results are displayed in a tabular format.  

---

## Project Setup and Execution  

### Prerequisites  

- Python 3.7 or higher  
- MongoDB instance (local or cloud)  
- Amazon RDS instance with a MySQL-compatible database  

### Steps to Run  

1. Clone the repository.  
2. Navigate to the project directory.  
3. Install required Python libraries using:  
   ```bash
   pip install -r requirements.txt
   ```
4. Set up the `.env` file in the project directory with the following content:  

   ```
   # MongoDB Configuration
   MONGODB_URI=<your_mongodb_uri>

   # Amazon RDS Configuration
   RDS_HOST=<rds_host>
   RDS_PORT=3306
   RDS_USER=<rds_user>
   RDS_PASSWORD=<rds_password>

   API_KEY="GeminiAPIKey"
   ```
5. Start the application:  
   ```bash
   python app.py
   ```
   This starts the backend server.  

---

## Uploading Data  

1. Prepare a folder containing all the CSV files.  
   - **Folder Name**: Will be treated as the database name.  
   - **File Names**: Each CSV file name will become a table (in Amazon RDS) or a collection (in MongoDB).  

2. Use the dashboard to upload the folder.  

   - MongoDB: Each CSV is stored as a **collection** in a MongoDB database named after the folder.  
   - Amazon RDS: Each CSV is stored as a **table** in a MySQL-compatible database named after the folder.  

---

## Querying Data  

- Use the dashboard to write human-like queries.  
- The backend processes the query and executes:  
  - SQL queries in Amazon RDS.  
  - MongoDB queries in MongoDB.  
- Results are displayed in a table format.  

---

## Technologies Used  

- **Backend**: Python (Flask)  
- **Database**:  
  - MongoDB Atlas  
  - Amazon RDS (MySQL-compatible)  
- **Frontend**: HTML, CSS, JavaScript  
- **Query Translation**: Regex-based NLP logic for human-like query conversion  

---

## Contributors  

- **Darshan Rao**  
  - Set up the backend architecture.  
  - Developed and integrated algorithms for handling queries.  
  - Implemented API calls for communication between the frontend and backend.  
  - Configured and managed Amazon RDS and MongoDB connections.  

- **Shardul Datar**  
  - Designed and developed the interactive user interface using HTML, CSS, and JavaScript.  
  - Implemented features for uploading entire databases and typing in human-like queries.  

- **Ekaterina Shtyrkova**  
  - Built the query transformation logic for converting natural-language-like queries into SQL and MongoDB queries.  
  - Developed regex templates to map user queries to corresponding database query patterns.  

---

## Folder Structure  

```
├── app.py                  # Main backend script
├── requirements.txt        # Python dependencies
├── .env.example            # Example environment file
├── uploads/                # Temporary storage for uploaded data
└── README.md               # Project documentation
```

---

## Future Enhancements  

- Add support for more database systems (e.g., PostgreSQL, DynamoDB).  
- Improve NLP capabilities for better query understanding.  
- Implement query history and analytics features.  

---

Feel free to contribute by submitting issues or pull requests!
