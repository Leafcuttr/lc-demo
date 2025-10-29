#!/bin/sh

API_ENDPOINT="http://localhost:18000/topics/jsontest"
JSON_PAYLOAD='{"records":[{"key":"someKeyText", "value":{"name": "testUser"}}, {"key":"someKeyText", "value":{"name": "testUser2"}}]}'

while true; do
    echo "Sending POST request to $API_ENDPOINT at $(date)"
    
    curl -s -S -X POST \
        -H "Content-Type: application/json" \
        -H "Accept: application/json" \
        -d "$JSON_PAYLOAD" \
        "$API_ENDPOINT"
        
    # Check the exit status of curl
    if [ $? -eq 0 ]; then
        echo "Request successful."
        sleep .2
    else
        echo "Request failed with status $?."
        echo Waiting for a few moments ...
        sleep 2
    fi
done