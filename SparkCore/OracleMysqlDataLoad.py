from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
uname="myuser"
pwd="mypassword"

df=spark.read.format("jdbc").option("url",host).option("dbtable","emp").option("user",uname).option("password",pwd).option("driver","com.mysql.jdbc.Driver").load()
#res=df.fill(0,"comm")
#res.show()
'''
res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))\

res.write.mode("overwrite").format("jdbc").option("url",host)\
    .option("dbtable","empclean").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()

res.show()
res.printSchema()
'''