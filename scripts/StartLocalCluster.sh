#!/usr/bin/env bash

rm -rf /tmp/sgdb*

~/Documents/gitrepos/simplegdb-Java/release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 0 -type edgecut -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid

~/Documents/gitrepos/simplegdb-Java/release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 1 -type edgecut -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid

~/Documents/gitrepos/simplegdb-Java/release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 2 -type edgecut -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid