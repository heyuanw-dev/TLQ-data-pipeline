#!/bin/bash

# JSON object to pass to Lambda Function
json="SELECT name FROM data;"

echo "Invoking Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name querymysql \
            --region us-east-2 --payload "{\"sql\": \"$json\"}" /dev/stdout | head -n 1 | head -c -2 ; echo`

results=$(echo "$output" | jq '.results')
clean_results=$(echo $results | sed 's/\\//g' | sed 's/^"\(.*\)"$/\1/')
echo $clean_results | jq . > results.json

echo ""
echo "JSON RESULT:"
echo $output | jq 'del(.results)'