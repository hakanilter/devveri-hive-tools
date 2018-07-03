#!/bin/bash
CLUSTER=development
HIVE_SERVER=localhost:10000

echo Started generating DDL scripts...
time java -cp lib/devveri-hive-tools-0.0.1-SNAPSHOT-dist.jar com.devveri.hive.tool.ClusterAnalyzerTool $HIVE_SERVER $CLUSTER > logs/generate-all.log 2>&1
echo Done!
