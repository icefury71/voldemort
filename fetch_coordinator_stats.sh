#! /bin/bash

[ $# -ne 3 ] && echo "Usage: ./fetch_coordinator_stats.sh <jmx-term jar path> <hostname> <time-in-minutes>" && exit -1

JMX_TERM_JAR_PATH=$1
HOSTNAME=$2
TIME_IN_MINS=$3

(( TOTAL_SECONDS = TIME_IN_MINS * 60 ))
OUTPUT_FILE="coordinator_stats.out"
echo "" >> $OUTPUT_FILE
date >> $OUTPUT_FILE


START_TIME_EPOCH=`date +%s`
CUR_TIME_EPOCH=`date +%s`
(( DIFF = CUR_TIME_EPOCH - START_TIME_EPOCH ))

echo "Start time = $START_TIME_EPOCH ... total seconds = $TOTAL_SECONDS"
for (( ;; ))
do
  CMD="get -b voldemort.coordinator:type=FatClientWrapper-test numberOfActiveThreads queuedRequests numberOfThreads"
  echo $CMD  | java -jar ${JMX_TERM_JAR_PATH} -l ${HOSTNAME}:7777 -v silent -n | tr -d "\n" >> ${OUTPUT_FILE}
  echo "" >> $OUTPUT_FILE
  CUR_TIME_EPOCH=`date +%s`
  (( DIFF = CUR_TIME_EPOCH - START_TIME_EPOCH ))
  [ $DIFF -gt $TOTAL_SECONDS ] && break
  echo "Curr time epoch =  $CUR_TIME_EPOCH and diff = $DIFF and total seconds = $TOTAL_SECONDS"
done
