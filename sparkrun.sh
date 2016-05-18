#!/bin/bash 
MASTER_SPARK_ADDR=$1
CORES=$2
MEMORY=$3
HOST=$(hostname)
echo "WORKER $HOST"
$SPARK_SBIN/start-slave.sh $MASTER_SPARK_ADDR --cores $CORES --memory $MEMORY

