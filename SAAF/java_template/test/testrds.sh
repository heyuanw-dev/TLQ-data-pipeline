#!/bin/bash

LOG_FILE_1="rds_transfer_times.txt"
LOG_FILE_2="rds_load_times.txt"
LOG_FILE_3="rds_query_times.txt"
# Service #1 Transform
for i in {1..36}; do
    echo "==============================number of run $i============================="
    json={"\"bucketname\"":\"test.bucket.562project.hw\"","\"filename\"":\"input/100000SalesRecords.csv\""}
    echo "Invoking TRANSFORM Lambda function using AWS CLI"
    (time output=`aws lambda invoke --invocation-type RequestResponse --function-name transform \
                --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`) 2>> $LOG_FILE_1
    echo ""
    echo "JSON RESULT:"
    echo $output | jq

    # Service #2 Load
    input={"\"bucketname\"":\"test.bucket.562project.hw\"","\"filename\"":\"output/TransformedSalesData.csv\""}
    echo "Invoking LOAD Lambda function using AWS CLI"
    (time output=`aws lambda invoke --invocation-type RequestResponse --function-name loadDB \
                --region us-east-2 --payload "$input" /dev/stdout | head -n 1 | head -c -2 ; echo`) 2>> $LOG_FILE_2
    echo ""
    echo "JSON RESULT:"
    echo $output | jq

    # Service #3 Query
    sql="SELECT Count(*) FROM sales_data WHERE Region='Australia and Oceania' AND Item_type='Office Supplies';"
    echo "Invoking QUERY Lambda function using AWS CLI"
    (time output=`aws lambda invoke --invocation-type RequestResponse --function-name queryRDS \
                --region us-east-2 --payload "{\"sql\": \"$sql\"}" /dev/stdout | head -n 1 | head -c -2 ; echo`) 2>> $LOG_FILE_3

    results=$(echo "$output" | jq '.results')
    clean_results=$(echo $results | sed 's/\\//g' | sed 's/^"\(.*\)"$/\1/')
    echo $clean_results | jq . > results.json
    echo ""
    echo "JSON RESULT:"
    echo $output | jq 'del(.results)'

    sleep 600
done