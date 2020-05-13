#!/bin/bash

script_date=python -c "import datetime; datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime("%F")")
echo $script_date
#today = date.today()
#yesterday = today - timedelta(days = 1)


#yest=$(date --date="yesterday")
#echo "$yest"
#yest=$(date --date="yesterday" +"%d/%m/%Y")
#echo "The backup was last verified on $yest"

#curl -H "Content-Type: application/json" -X POST http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches -d "$json_var"
#
#response=$(curl -H "Content-Type: application/json" -X POST http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches -d "$json_var")
#echo "getting id"
#echo "$response" | python -c "import sys, json; print json.load(sys.stdin)['id']"
#session_id=$(echo "$response" | python -c "import sys, json; print json.load(sys.stdin)['id']")
#echo "session_id: $session_id"
#
#url_check_state="http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches/${session_id}/state)"
#curl -X GET "$url_check_state"
#
#response2=$(curl -X GET http://ec2-54-147-14-244.compute-1.amazonaws.com:8998/batches/${session_id}/state)
#echo "response2: $response2"
#
#
#final_state_response=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")
#echo "final_state_response: $final_state_response"sponse=$(echo "$response2" | python -c "import sys, json; print json.load(sys.stdin)['state']")