curl -X POST \
  http://localhost:9047/api/v3/sql \
  -H 'Authorization: token' \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "SELECT * FROM Samples.\"samples.dremio.com\".\"SF_incidents2016.json\" LIMIT 3"
  }'
