#!/usr/bin/env bash
export HIVE_SERVER="jdbc:hive2://quickstart.cloudera:10000/"
export IMPALA_SERVER="jdbc:hive2://quickstart.cloudera:21050/;auth=noSasl"
export JAVA_OPTS="" # add "-Dhadoop.security.authentication=kerberos" for kerberos auth
export JAR_FILE="lib/devveri-hive-tools-0.0.1-SNAPSHOT-dist.jar"
