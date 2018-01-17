#!/bin/bash

if [ -f "/home/cloudera/Assignment/musicProject/logs/current-batch.txt" ]
then
 echo "Batch File Found!"
else
 echo -n "1" > "/home/cloudera/Assignment/musicProject/logs/current-batch.txt"
fi

chmod 775 /home/cloudera/Assignment/musicProject/logs/current-batch.txt

batchid=`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
echo $batchid
LOGFILE=/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid

echo "Checking demons in cloudera vm" >> $LOGFILE
jps
