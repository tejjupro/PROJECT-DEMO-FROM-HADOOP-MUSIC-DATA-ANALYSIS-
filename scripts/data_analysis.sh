#!/bin/bash

batchid=`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`

LOGFILE=/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid

echo "Running Spark Script for Data Analysis..." >> $LOGFILE

echo "Exporting analyzed data to Local FS..." >> $LOGFILE

cat /home/cloudera/Assignment/musicProject/scripts/data_analysis.scala | spark-shell

echo "All Activities Complete..." >> $LOGFILE

echo "Incrementing batchid..." >> $LOGFILE

echo "Incrementing batchid..." >> $LOGFILE

batchid=`expr $batchid +1`

echo -n $batchid > /home/cloudera/Assignment/musicProjects/logs/current-batch.txt
