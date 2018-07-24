#!/bin/bash
source ./environment.sh

DATABASE=$1
if [ -z "$1" ]
  then
    echo "Please provide a database name as an argument"
    exit
fi

time java $JAVA_OPTS -cp $JAR_FILE com.devveri.hive.tool.DatabaseReportTool "$HIVE_SERVER" $DATABASE
