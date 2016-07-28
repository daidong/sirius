#!/usr/bin/env bash

concurrent=1
op="insert"
graph_file="/proj/cloudincr-PG0/tools/graphmeta/rmat1m128-256"

while [[ $# > 1 ]]
do
    key="$1"

    case $key in
        -n | --number)
            server_number=$2
            shift
            ;;
        -g | --graph)
	    	graph_file=$2
	    	shift
	    	;;
        -o | --op)
	    	op="$2"
	    	shift
	    	;;
		-t | --type)
            server_type="$2"
            shift
            ;;
        -d | --db)
            db_dir="$2"
            shift
            ;;
		-c | --con)
	    	concurrent="$2"
	    	shift
	    	;;
        --default)
            echo default
            ;;
        *)
            ;;
    esac
    shift
done

bound=`expr ${server_number} - 1`
con=`expr ${concurrent} - 1`

port=5555
seeds=""
line=0
for i in $(seq 0 $bound)
do
    if [ "$line" -eq 0 ]
    then
        seeds="node-$i:$port"
    else
        seeds="$seeds node-$i:$port"
    fi
    line=`expr 1 + $line`
done

localdir=${db_dir}
localdb=${localdir}/gfsdb
id=0

~/iogp-code/release/iogp-0.1/bin/client.sh -type ${server_type} -op $op -id $id -graph ${graph_file} -srvlist $seeds

#for i in $(seq 0 $bound)
#do
#    for j in $(seq 0 $con)
#    do
#	id=`expr $i \* $concurrent`
#	id=`expr $id + $j`
#	ssh node-$i "~/iogp-code/release/iogp-0.1/bin/client.sh -type ${server_type} -op $op -id $id -graph
#${graph_file} -srvlist $seeds" &
#   done
#done
