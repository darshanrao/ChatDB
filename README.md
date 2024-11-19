# ChatDB
This readme will be updated later
Now just used for taking note
-Backend
-Completed with the upload function on Mongodb




Sample queries

curl -X POST \
  http://127.0.0.1:5000/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "db.students.aggregate([{ \"$group\": { \"_id\": \"$Major\", \"studentCount\": { \"$sum\": 1 } } }])"
  }'