#!/bin/bash

batchid=`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
LOGFILE=/home/cloudera/Assignment/musicProject/logs/log_batch_batchid

echo "Creating hive tables on top of hbase tables for data enrichment and filtering..." >> $LOGFILE

hive -f /home/cloudera/Assignment/musicProject/scripts/create_hive_hbase_lookup.hql
