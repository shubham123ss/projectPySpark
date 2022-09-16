from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="s3://shubhambkt2023/sqoop/output/part-00000-8c78a201-56fd-4edb-97a8-0cced3711085-c000.csv"
df=spark.read.format("csv").option("header","True").load(data)
df.show()