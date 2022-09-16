from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="D:\\AVD\\SPARK\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",",").load(data)

import re
cols=[re.sub('[^a-zA-Z0-9]',"",c) for c in df.columns]
ndf=df.toDF(*cols)

ndf.show(21,truncate=True)
df.printSchema()