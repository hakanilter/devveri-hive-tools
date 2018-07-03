#!/bin/bash
if [ -z "$1" ]
  then
    echo "Please provide a database name as an argument"
    exit
fi

DATABASE=$1
HIVE_SERVER=localhost:10000

echo Started generating DDL scripts for $DATABASE
time java -cp lib/devveri-hive-tools-0.0.1-SNAPSHOT-dist.jar com.devveri.hive.tool.DatabaseAnalyzerTool $HIVE_SERVER $DATABASE > logs/generate.log 2>&1
echo Done!
