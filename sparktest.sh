#!/bin/bash 
#SBATCH -N 2 
#SBATCH -t 00:10:00 
#SBATCH --mem 4000 
#this DOES change number of processes created below
#SBATCH --ntasks-per-node 1 
#this does not change number of processes created below
#SBATCH --cpus-per-task 8
export SPARK_HOME=/home/jpb67/spark-1.6.1-bin-hadoop2.6
export SPARK_SBIN=$SPARK_HOME/sbin
export SPARK_BIN=$SPARK_HOME/bin
module load jdk/1.8.0_45-fasrc01
module load python/2.7.9-fasrc01
MASTER=$(hostname)
$SPARK_SBIN/start-master.sh
MASTER_SPARK_ADDR="spark://$MASTER:7077"
echo $MASTER > /home/jpb67/master.txt
echo "MASTER_SPARK_ADDR:$MASTER_SPARK_ADDR"
srun sparkrun.sh $MASTER_SPARK_ADDR 8 4G
sleep 40
#SCRIPT=$SPARK_HOME/examples/src/main/python/pi.py
SCRIPT=./jpb.py
$SPARK_BIN/spark-submit --master $MASTER_SPARK_ADDR --total-executor-cores 16 --executor-memory 4G --packages com.databricks:spark-csv_2.10:1.4.0 $SCRIPT
sleep 30
$SPARK_SBIN/stop-master.sh
