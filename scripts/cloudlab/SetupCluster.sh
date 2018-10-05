#!/usr/bin/env bash

# This script should run in head node
while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRGDIR=`dirname "$PRG"`
[ -z "$PROCESSOR_HOME" ] && PROCESSOR_HOME=`cd "$PRGDIR" ; pwd`

bound=`expr $1 - 1`

# Touch Every Server
for i in $(seq 0 $bound)
do
	ssh-keyscan node$i >> ~/.ssh/known_hosts
done

# Install Packages
for i in $(seq 0 $bound)
do
	ssh -t node$i "$PROCESSOR_HOME/package-installer.sh" &
done

echo "$PROCESSOR_HOME"