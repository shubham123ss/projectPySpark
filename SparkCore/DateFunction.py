from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()
sc = spark.sparkContext

data="D:\\AVD\\SPARK\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",",").load(data)
'''
=======>>>> By default spark able to understand 'yyyy-MM-dd' format only
But in our dataset dd-MM-yyyy so we have to conver that format to spark understandable format
===> current_date() --> used to get current date on your system
========>>>> VVIMP <<<<========
.config("spark.sql.session.timeZone","EST") --->> its very important based on orignal cilent data all default timebased on US time only. at that time mention "EST"
--> current_timestamp() it will give date with current time in min. & sec.
'''


res=df.withColumn("dt",to_date(df.dt,"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),1000))\
    .withColumn("dtsub",date_sub(col("dt"),1000))\
    .withColumn("lastdt",date_format(last_day(col("dt")),"yyyy-MM-dd-EEE"))\
    .withColumn("nxtday",next_day(col("dt"),"Sun"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMM/yy"))\
    .withColumn("MntLstFriday",next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("dayOfWeek",dayofweek(col("dt")))\
    .withColumn("dayOfMonth",dayofmonth(col("dt")))\
    .withColumn("dayOfYear",dayofyear(col("dt")))\
    .withColumn("yr",year(col("dt")))\
    .withColumn("yr",month(col("dt")))\
    .withColumn("MonBetween",months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("MonBetween")))\
    .withColumn("celing",ceil(col("MonBetween")))\
    .withColumn("round",round(col("MonBetween")))
res.printSchema()
res.show(truncate=False)