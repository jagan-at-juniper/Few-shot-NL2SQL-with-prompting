#!/bin/bash

response=$(curl -H "Content-Type: application/json" -X POST http://ec2-3-93-213-4.compute-1.amazonaws.com:8998/batches -d '{"className": "org.apache.spark.examples.SparkPi", "args": ["blah"], "file": "s3://mist-data-science-dev/ryanr/action_report.py"}')

echo "getting id"
echo "$response" | python -c "import sys, json; print json.load(sys.stdin)['id']"
session_id=$(echo "$response" | python -c "import sys, json; print json.load(sys.stdin)['id']")
echo "session_id: $session_id"

url_check_state="http://ec2-3-93-213-4.compute-1.amazonaws.com:8998/batches/${session_id}/state)"
curl -X GET "$url_check_state"

response2=$(curl -X GET http://ec2-3-93-213-4.compute-1.amazonaws.com:8998/batches/${session_id}/state)
echo "response2: $response2"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"

final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
echo "final_state_response: $final_state_response"