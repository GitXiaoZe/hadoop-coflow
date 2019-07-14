#!/bin/bash

hdfs dfs -get /tmp/logs .
mkdir templog
cur_dir=`pwd`
prefix="/logs/tian/logs/"
applications=`ls ${cur_dir}${prefix}`
for application in ${applications}
do
	strings ${cur_dir}${prefix}${application}/* > templog/${application}.txt
	grep -E "Counters:|HDFS: Number of bytes|Reduce shuffle bytes|Map output materialized bytes|CPU time spent|time " templog/${application}.txt > templog/${application}-summary.txt
done
rm -r logs
