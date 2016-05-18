from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DecimalType, DoubleType

elk1_filename = 'data/ELK1.bed'

def create_tsv_table(sqlContext, path, schema, table_name):
    csv_loader = sqlContext.read.format("com.databricks.spark.csv")
    csv_loader.option("delimiter", "\t")
    df = csv_loader.load(path, schema=schema)
    df.registerTempTable(table_name)

def prediction_schema():
    return StructType([
        StructField("chrom", StringType(), True),
        StructField("start", IntegerType(), True),
        StructField("end", IntegerType(), True),
        StructField("value", DoubleType(), True),
        StructField("other", IntegerType(), True)])

def setup_prediction_table(sqlContext):
    create_tsv_table(sqlContext, "data/ELK1.bed", prediction_schema(), "prediction")


sc = SparkContext(appName="PythonSQL")
sqlContext = SQLContext(sc)
setup_prediction_table(sqlContext)

sql= """
SELECT chrom, max(value) as max, min(value) as min, avg(value) as avg FROM prediction group by chrom
"""
selectedData = sqlContext.sql(sql)
print("RESULTS")
print("--------------")
for each in selectedData.collect():
   print(each)
print("--------------")

