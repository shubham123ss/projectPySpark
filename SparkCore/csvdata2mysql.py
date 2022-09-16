from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="D:\\AVD\\SPARK\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",",").load(data)

import re
cols=[re.sub('[^a-zA-Z0-9]',"",c) for c in df.columns]
ndf=df.toDF(*cols)
ndf.show()
ndf.show(21,truncate=False)
df.printSchema()

host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
uname="myuser"
pwd="mypassword"

ndf.write.mode("overwrite").format("jdbc").option("url",host)\
    .option("dbtable","shubhampoc").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()