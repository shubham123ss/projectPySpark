from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").enableHiveSupport().getOrCreate()
Access_key_ID="XXXX"
Secret_access_key="XXXX"
# Enable hadoop s3a settings
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

#mySQL conncetion details (where we want to store the data)
host="jdbc:mysql://mysqldb.cjyrchmwhfjw.us-east-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname="myuser"
pwd="mypassword"

#(loading the data from directly to our s3 resource into the dataframe)
data="s3a://hiveproj/historical_data/historical_data.csv"
df=spark.read.format('csv').option("header","true").option("inferSchema","true").load(data)

#creating a table as history in sparkSQL and saving the data in history table
df.createOrReplaceTempView("history")
ndf=spark.sql("select created_at, actual_delivery_time,(bigint(to_timestamp(actual_delivery_time))) - (bigint(to_timestamp(created_at))) as time_diff from history")

#ndf.write.mode("overwrite").format("jdbc").option("url",host).option("dbtable","time_diff").option("user",uname).option("password",pwd).option("driver","com.mysql.jdbc.Driver").save()
df.write.saveAsTable("historical_data")
ndf.show(100,truncate=False)
ndf.printSchema()

#s3://hiveproj/historical_data/historical_data.csv
