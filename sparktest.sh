#!/bin/bash 
#SBATCH -N 2 
#SBATCH -t 00:03:00 
#SBATCH --mem 1000 
#this DOES change number of processes created below
#SBATCH --ntasks-per-node 1 
#this does not change number of processes created below
#SBATCH --cpus-per-task 4
export SPARK_HOME=/home/jpb67/spark-1.6.1-bin-hadoop2.6
export SPARK_SBIN=$SPARK_HOME/sbin
export SPARK_BIN=$SPARK_HOME/bin
module load jdk/1.8.0_45-fasrc01
module load python/2.7.9-fasrc01
MASTER=$(hostname)
$SPARK_SBIN/start-master.sh
sleep 5
MASTER_SPARK_ADDR="spark:$MASTER:7077"
echo "MASTER_SPARK_ADDR:$MASTER_SPARK_ADDR"
srun sparkrun.sh $MASTER_SPARK_ADDR 4 1G
SCRIPT=$SPARK_HOME/examples/src/main/python/pi.py
#SCRIPT=./jpb.py
$SPARK_BIN/spark-submit --total-executor-cores 8 --executor-memory 1G --packages com.databricks:spark-csv_2.11:1.4.0 $SCRIPT
$SPARK_SBIN/stop-master.sh
