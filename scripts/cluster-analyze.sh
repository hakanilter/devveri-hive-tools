#!/usr/bin/env bash
source ./environment.sh

CLUSTER=$1
if [ -z "$1" ]
  then
    CLUSTER="cluster"
fi

time java $JAVA_OPTS -cp $JAR_FILE com.devveri.hive.tool.analyzer.ClusterAnalyzerTool "$HIVE_SERVER" $CLUSTER
