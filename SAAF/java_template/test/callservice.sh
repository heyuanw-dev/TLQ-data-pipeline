#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"bucketname\"":\"tql.transform\"","\"filename\"":\"input/100SalesRecords.csv\""}
echo "Invoking Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://jvvl6wgcxe.execute-api.us-east-2.amazonaws.com/Tql_stage`
echo ""
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
