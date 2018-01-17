#!/bin/bash

batchid=`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
LOGFILE=/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid
VALIDDIR=/home/cloudera/Assignment/musicProject/processed_dir/valid/batch_$batchid
INVALIDDIR=/home/cloudera/Assignment/musicProject/processed_dir/invalid/batch_$batchid

echo $batchid
echo "Running hive script for data enrichment and filtering ..." >> $LOGFILE

hive -hiveconf batchid=$batchid -f /home/cloudera/Assignment/musicProject/scripts/data_enrichment.hql

if [ ! -d "$VALIDDIR" ]
then
mkdir -p "$VALIDDIR"
fi

if [ ! -d "$INVALIDDIR" ]
then
mkdir -p "$INVALIDDIR"
fi

echo "Copying valid and invalid records in local file system ..." >> $LOGFILE

hdfs dfs -get /user/hive/warehouse/project.db/enriched_data/batchid=$batchid/status=pass/* $VALIDDIR
hdfs dfs -get /user/hive/warehouse/project.db/enriched_data/batchid=$batchid/status=fail/* $INVALIDDIR

echo "Deleting older valid and invalid records from local file system..." >> $LOGFILE

find /home/cloudera/Assignment/musicProject/processed_dir/ -mtime +7 -exec rm {} \;
