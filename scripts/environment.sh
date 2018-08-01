#!/usr/bin/env bash
export HIVE_SERVER=localhost:10000
export IMPALA_SERVER=localhost:21050
export JAVA_OPTS="" # add "-Dhadoop.security.authentication=kerberos" for kerberos auth
export JAR_FILE="lib/devveri-hive-tools-0.0.1-SNAPSHOT-dist.jar"
