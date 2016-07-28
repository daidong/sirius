#!/usr/bin/env bash

# This script should run in head node

# Setup Head Node
cp /proj/cloudincr-PG0/tools/installers/id_rsa ~/.ssh/
cp /proj/cloudincr-PG0/tools/installers/id_dsa ~/.ssh/
ssh-agent bash
ssh-add

bound=`expr $1 - 1`

# Touch Every Server
for i in $(seq 0 $bound)
do
	ssh-keyscan node-$i >> ~/.ssh/known_hosts
done

# Install Packages
for i in $(seq 0 $bound)
do
	ssh -t node-$i "./package-installer.sh" &
done