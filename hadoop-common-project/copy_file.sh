#!/bin/bash

cd hadoop-common/target/hadoop-common-3.1.1/share/hadoop/common 
cp *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/
pdcp -w ssh:tian@172.16.0.[2-10] *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/

cd -
cd hadoop-kms/target/hadoop-kms-3.1.1/share/hadoop/common
cp *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/
pdcp -w ssh:tian@172.16.0.[2-10] *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/

cd -
cd hadoop-nfs/target/hadoop-nfs-3.1.1/share/hadoop/common
cp *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/
pdcp -w ssh:tian@172.16.0.[2-10] *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/common/

