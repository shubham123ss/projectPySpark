from pyspark.sql import *
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="s3://drivers2022/asl.csv"
df=spark.read.format("csv").option("header","True").load(data)
df.show()
