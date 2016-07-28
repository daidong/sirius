#!/usr/bin/env bash

rm -rf /tmp/iogpdb*

../release/iogp-0.1/bin/server.sh start -db /tmp/iogpdb -id 0 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/iogpsrv.pid

../release/iogp-0.1/bin/server.sh start -db /tmp/iogpdb -id 1 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/iogpsrv.pid

../release/iogp-0.1/bin/server.sh start -db /tmp/iogpdb -id 2 -type iogp -srvlist 127.0.0.1:5555 127.0.0.1:5556 127.0.0.1:5557
rm /tmp/iogpsrv.pid



