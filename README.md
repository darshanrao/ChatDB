# ChatDB

This readme will be updated later
Now just used for taking note
-Backend
-Completed with the upload function on Mongodb

Sample queries
User Query format: 'sample queries for' + operation

curl -X POST \
 http://127.0.0.1:5000/api/query-mongodb \
 -H 'Content-Type: application/json' \
 -d '{
"query": "db.students.aggregate([{ \"$group\": { \"_id\": \"$Major\", \"studentCount\": { \"$sum\": 1 } } }])"
}'

curl -X POST \
 http://127.0.0.1:5000/api/query-mongodb \
 -H 'Content-Type: application/json' \
 -d '{
"query": "db.students.aggregate([{ \"$lookup\": { \"from\": \"enrollments\", \"localField\": \"StudentID\", \"foreignField\": \"StudentID\", \"as\": \"enrollments\" } }, { \"$lookup\": { \"from\": \"courses\", \"localField\": \"enrollments.CourseID\", \"foreignField\": \"CourseID\", \"as\": \"courses\" } }, { \"$match\": { \"$expr\": { \"$in\": [\"$AdvisorName\", \"$courses.InstructorName\"] } } }, { \"$project\": { \"FirstName\": 1, \"LastName\": 1, \"AdvisorName\": 1 } }])"
}'

curl -X POST \
 -F "db_name=database2" \
 -F "files=@data/courses.csv" \
 -F "files=@data/enrollments.csv" \
 -F "files=@data/students.csv" \
 http://127.0.0.1:5000/api/upload-mongodb
