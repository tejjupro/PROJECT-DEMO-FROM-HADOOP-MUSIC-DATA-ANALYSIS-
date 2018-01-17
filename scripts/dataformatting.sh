#!/bin/bash

batchid=`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
echo $batchid
LOGFILE=/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid

echo "Placing data files from local to HDFS..." >> $LOGFILE

hdfs dfs -rm -r /user/cloudera/project/batch${batchid}/web/
hdfs dfs -rm -r /user/cloudera/project/batch${batchid}/formattedweb/
hdfs dfs -rm -r /user/cloudera/project/batch${batchid}/mob/

hdfs dfs -mkdir -p /user/cloudera/project/batch${batchid}/web/
hdfs dfs -mkdir -p /user/cloudera/project/batch${batchid}/mob/

hdfs dfs -put /home/cloudera/Assignment/musicProject/data/web/* /user/cloudera/project/batch${batchid}/web/
hdfs dfs -put /home/cloudera/Assignment/musicProject/data/mob/* /user/cloudera/project/batch${batchid}/mob/

echo "Running pig script for data formating..." >> $LOGFILE

pig -param batchid=$batchid /home/cloudera/Assignment/musicProject/scripts/dataformatting.pig

echo "running hive script for formatted data load..." >> $LOADFILE

hive -hiveconf batchid=$batchid -f /home/cloudera/Assignment/musicProject/scripts/formatted_hive_load.hql
