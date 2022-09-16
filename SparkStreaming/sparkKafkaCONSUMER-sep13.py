from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "indpak") \
  .load()
ndf=df.selectExpr("CAST(value AS STRING)")
#ndf=df.withColumn("value","CAST(value AS STRING)")
ndf.printSchema()
#query = ndf.writeStream.format("console").start()
ex=r'^(\S+),(\S+),(\S+)'
res=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias("age"),regexp_extract('value',ex,3).alias("city"))
query = res.writeStream.format("console").start()
def foreach_batch_function(df, bid):
  host = "jdbc:mysql://mysqldb.cjyrchmwhfjw.us-east-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
  df.write.mode("append").format("jdbc") \
    .option("url", host) \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "kafkalivedata").save()

  pass

res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#query = res.writeStream.format("console").start()
#query.awaitTermination()