#!/usr/bin/env bash
source ./environment.sh

DATABASE=$1
if [ -z "$1" ]
  then
    echo "Please provide a database name as an argument"
    exit
fi

time java $JAVA_OPTS -cp $JAR_FILE com.devveri.hive.tool.health.DatabaseHealthTool "$IMPALA_SERVER" $DATABASE
