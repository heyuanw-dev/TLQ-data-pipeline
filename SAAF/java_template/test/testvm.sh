#!/bin/bash

# Service #1 Transform
json={"\"bucketname\"":\"test.bucket.562project.hw\"","\"filename\"":\"input/50000SalesRecords.csv\""}
echo "Invoking TRANSFORM Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name transform \
            --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq

# Service #2 Load
input={"\"bucketname\"":\"test.bucket.562project.hw\"","\"filename\"":\"output/TransformedSalesData.csv\""}
echo "Invoking LOAD Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name loadVM \
            --region us-east-2 --payload "$input" /dev/stdout --cli-read-timeout 0 | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq

# Service #3 Query
sql="SELECT SUM(Units_sold) FROM sales_data WHERE Region='Australia and Oceania' AND Item_type='Office Supplies';"
echo "Invoking QUERY Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name queryVM \
            --region us-east-2 --payload "{\"sql\": \"$sql\"}" /dev/stdout | head -n 1 | head -c -2 ; echo`

results=$(echo "$output" | jq '.results')
clean_results=$(echo $results | sed 's/\\//g' | sed 's/^"\(.*\)"$/\1/')
echo $clean_results | jq . > results_2.json
echo ""
echo "JSON RESULT:"
echo $output | jq 'del(.results)'
