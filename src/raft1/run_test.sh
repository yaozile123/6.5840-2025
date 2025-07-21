#!/bin/bash

KEYWORD=$1
NUM_RUNS=$2
OUTPUT_FILE="output.log"

echo "Starting test runs" > $OUTPUT_FILE

for ((i=1; i<=NUM_RUNS; i++))
do
    echo "===== Run $i: $KEYWORD =====" | tee -a $OUTPUT_FILE
    go test -run $KEYWORD -race | tee -a $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
done

echo "All done. Log saved to $OUTPUT_FILE"
