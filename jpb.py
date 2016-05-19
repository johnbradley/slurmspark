from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DecimalType, DoubleType


def create_tsv_table(sqlContext, path, schema, table_name):
    csv_loader = sqlContext.read.format("com.databricks.spark.csv")
    csv_loader.option("delimiter", "\t")
    df = csv_loader.load(path, schema=schema)
    df.registerTempTable(table_name)

def setup_prediction_table(sqlContext):
    elk1_filename = 'data/ELK1*.bed'
    prediction_schema = StructType([
        StructField("chrom", StringType(), True),
        StructField("start", IntegerType(), True),
        StructField("end", IntegerType(), True),
        StructField("value", DoubleType(), True),
        StructField("other", IntegerType(), True)])
    create_tsv_table(sqlContext, elk1_filename, prediction_schema, "prediction")

def setup_gene_table(sqlContext):
    gene_filename = 'data/knownGene.txt'
    gene_schema = StructType([
        StructField("name", StringType(), True),
        StructField("chrom", StringType(), True),
        StructField("strand", StringType(), True),
        StructField("txStart", IntegerType(), True),
        StructField("txEnd", IntegerType(), True),
        StructField("cdsStart", IntegerType(), True),
        StructField("cdsEnd", IntegerType(), True),
        StructField("exonCount", IntegerType(), True),
        StructField("exonStarts", StringType(), True),
        StructField("exonEnds", StringType(), True),
        StructField("proteinID", StringType(), True),
        StructField("alignID", StringType(), True)])
    create_tsv_table(sqlContext, gene_filename, gene_schema, "gene")


sc = SparkContext(appName="PredictionLookup")
sqlContext = SQLContext(sc)
setup_prediction_table(sqlContext)
setup_gene_table(sqlContext)

sql= """
SELECT gene.name, gene.chrom, gene.strand, gene.txStart, gene.txEnd, prediction.start, prediction.value
FROM prediction 
inner join gene on 
prediction.chrom = gene.chrom
and prediction.start > gene.txStart
and prediction.start < gene.txEnd
where prediction.chrom = 'chr1'
limit 100
"""

selectedData = sqlContext.sql(sql)
#print("RESULTS")
#print("--------------")
#for each in selectedData.collect():
#   print(each)
#selectedData.write.format("json").save("resultdata")
writer = selectedData.write.format("com.databricks.spark.csv")
writer.option("delimiter", "\t")
writer.option("header", "true")
writer.save("resultdir")
#print("--------------")

#chr1	11454	11490	0.321928372788	321
#--packages com.databricks:spark-csv_2.11:1.4.0
