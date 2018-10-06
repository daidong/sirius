#!/bin/sh
#
# author zhangwei
PRG="$0"

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
[ -z "$PROCESSOR_HOME" ] && PROCESSOR_HOME=`cd "$PRGDIR/.." ; pwd`

SERVER_NAME=sgdb

# path
BIN_PATH=$PROCESSOR_HOME/bin
LOG_PATH=$PROCESSOR_HOME/logs
LIB_PATH=$PROCESSOR_HOME/lib
#
mkdir -p $LOG_PATH
touch $LOG_PATH/stdout.log

#
CLASS_NAME=edu.dair.sgdb.gserver.ServerMain
CLASS_PATH=$PROCESSOR_HOME/conf
#
for f in $LIB_PATH/*.jar
do
    CLASS_PATH=$CLASS_PATH:$f;
done

#DEBUG_ARGS="-agentlib:jdwp=transport=dt_socket,address=8759,server=y,suspend=y";
DEBUG_ARGS="";
#
PROGRAM_ARGS="-Xms4g -Xmx4g  -Xmn1g -Dapp.name=${SERVER_NAME} -Dapp.base=${PROCESSOR_HOME} -XX:+UseConcMarkSweepGC -server -XX:SurvivorRatio=5 -XX:CMSInitiatingOccupancyFraction=80 -XX:+PrintTenuringDistribution  -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime ${DEBUG_ARGS} -Xloggc:./gc.log"
PIDFILE=/tmp/sgdbsrv.pid

HOST=`hostname`
#
STDOUT=$LOG_PATH/stdout.${HOST}.log
STDERR=$LOG_PATH/stderr.${HOST}.log
#STDOUT=/dev/null
#STDERR=/dev/null

#if [ "$(uname)" == "Darwin" ]; then
#    JAVA_HOME=`/usr/libexec/java_home -v 1.7`
#fi

start()
{
if test -e $PIDFILE
        then
                echo The $SERVER_NAME Server already Started!
        else
                echo Start The $SERVER_NAME Server.... $@
                #>>$STDOUT 2>>$STDERR # $JAVA_HOME/bin/
                java $PROGRAM_ARGS -classpath $CLASS_PATH $CLASS_NAME -stopSign "${PROCESSOR_HOME}"/stopread $@ &
                echo $!>$PIDFILE
                sleep 2
                TPID=`cat $PIDFILE`
                STATUS=`ps -p $TPID |grep java | awk '{print $1}'`
                if test $STATUS
                        then
                                echo The $SERVER_NAME Server Started with pid=$STATUS!
				startreading
                        else
                                rm -f $PIDFILE
				echo The $SERVER_NAME Server Start Failed
                                echo please Check the system
                                echo
                fi
fi
}

stopread(){
	touch $PROCESSOR_HOME/stopread;
}

startreading(){
	rm -f $PROCESSOR_HOME/stopread;
}

stop()
{
if test -e $PIDFILE
        then
		echo Stop reading...
		stopread
		sleep 5s
                echo Stop The $SERVER_NAME Server....
                TPID=`cat $PIDFILE`
                kill -9 $TPID
                sleep 1
                STATUS=`ps -p $TPID |grep java | awk '{print $1}'`
                if test $STATUS
                        then
                                echo The $SERVER_NAME Server NOT Stoped!
                                echo please Check the system
                        else
                                echo The $SERVER_NAME Server Stoped
                                rm $PIDFILE
                fi
        else
                echo The $SERVER_NAME Server already Stoped!
fi
}



status()
{
echo
if test -e $PIDFILE
        then
                TPID=`cat $PIDFILE`
                STATUS=`ps -p $TPID|grep java | awk '{print $1}'`
                if test $STATUS
                        then
                             #   echo "The $SERVER_NAME Server Running($TPID)!"
                                echo
                        else
                             #   echo The $SERVER_NAME Server NOT Running!
                                rm $PIDFILE
                                echo
                fi
        else
                echo The $SERVER_NAME Server NOT Running!
fi
}

status
case "$1" in
'start')
                shift
                start $@
        ;;
'stop')
                stop
        ;;
'status')
                status
        ;;
'stopread')
		stopread
	;;
'startreading')
		startreading
	;;
*)
        echo
        echo
        echo "Usage: $0 {status | start | stop | stopread | startreading }"
        echo
        echo Status of $SERVER_NAME Servers ......
                status
        ;;
esac
exit 0
