#!/bin/bash

cd target/hadoop-yarn-project-3.1.1/share/hadoop/yarn
cp *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/yarn/
pdcp -w ssh:tian@172.16.0.[2-10] *.jar /home/tian/public/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/share/hadoop/yarn/
