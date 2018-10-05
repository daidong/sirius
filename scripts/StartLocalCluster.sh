#!/usr/bin/env bash

rm -rf /tmp/sgdb*

../release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 0 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid

../release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 1 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid

../release/sgdb-0.1/bin/server.sh start -db /tmp/sgdb -id 2 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/sgdbsrv.pid



