#!/usr/bin/env bash

bound=`expr $1 - 1`

# SOURCE
if [ "SOURCE" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo SOURCE Node$i
    	ssh node$i "source ~/.bashrc"
	done
fi

# RM
if [ "RM" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo RM Node$i
    	ssh node$i "rm -r ~/dbs/*"
	done
fi

# UPDATE
if [ "UPDATE" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo UPDATE Node$i
    	ssh node$i "cd ~/simplegdb-Java; git pull" &
	done
fi

# CLEAN
if [ "CLEAN" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo CLEAN Node$i
    	ssh node$i "cd ~/simplegdb-Java; make clean" &
	done
fi

# UPDATE and COMPILE
if [ "UPDATE-COMPILE" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo UPDATE Node$i
    	ssh node$i "cd ~/simplegdb-Java; git pull; make all" &
	done
fi

# SNTP
if [ "SNTP" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo SNTP Node$i
    	ssh -t node$i "sudo sntp -s 24.56.178.140" &
	done
fi

# JPS
if [ "JPS" = $2 ]; then
	for i in $(seq 0 $bound)
	do
    	echo JPS Node$i
    	ssh -t node$i "jps"
	done
fi

# File Limits
if [ "LIMIT" = $2 ]; then
	for i in $(seq 0 $bound)
	do
		echo Increase File Open Limit on Node$i
		ssh -t node$i "sudo cp /proj/dirr-PG0/tools/limits.conf /etc/security/limits.conf"
	done
fi

