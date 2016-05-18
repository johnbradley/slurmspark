# slurmspark
Researching running spark on a slurm cluster

## Step 1 - download and extract spark
I downloaded spark-1.6.1-bin-hadoop2.6. Do not download the version 'without' hadoop as that will not work, even though I am not using haddoop.

## Step 2 - setup configuration
Change all the paths in sparktest.sh to point to your location.

## Step 3 - run it
sbatch sparktest.sh 



