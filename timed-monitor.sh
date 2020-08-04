#!/bin/bash
# run database monitor session for specified timeframe, then kill

# usage:
# ./timed-monitor.sh <duration_in_sec> <redis db password>

SEC=$1
REDIS_AUTH=$2
echo "Monitor Duration: $SEC"

LOC="/tmp"
FNAME="monitor.$HOSTNAME.$(date +%F_%H-%M)"
OUT_FILE="$LOC/$FNAME.out"
TAR_FILE="$LOC/$FNAME.tar.gz"
echo "Output location: "$TAR_FILE"

redis-cli -h $SERVICE_HOST -p $SERVICE_PORT -a $REDIS_AUTH MONITOR > $OUT_FILE 2> /dev/null &
MON_PID=$!
echo "redis-cli monitor PID: $MON_PID"
sleep $SEC
kill -9 $MON_PID > /dev/null &
wait $!
tar --remove-files -czf $TAR_FILE $OUT_FILE 