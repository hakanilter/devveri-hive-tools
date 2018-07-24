#!/bin/bash
source ./environment.sh

DATABASE=$1
if [ -z "$1" ]
  then
    echo "Please provide a database name as an argument"
    exit
fi

TABLE=$2
if [ -z "$2" ]
  then
    echo "Please provide a table name as an argument"
    exit
fi

time java $JAVA_OPTS -cp $JAR_FILE com.devveri.hive.tool.TableHealthTool "$IMPALA_SERVER" $DATABASE $TABLE
