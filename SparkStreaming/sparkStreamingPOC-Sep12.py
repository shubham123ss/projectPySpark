from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
ssc= StreamingContext(spark.sparkContext, 10)
spark.sparkContext.setLogLevel("ERROR")

#spark internally uses different context to create different API
#sparkContext used to create RDD API
#sqlContext used to create DataFrame API
#sparkSession used to create dataset API
#sparkStreamingContext is used to create Dstream API

# create dstream ... its very headache process..
#socketTextStream is to get data from console/terminal from something server & port No.
host="ec2-18-204-231-42.compute-1.amazonaws.com"
lines = ssc.socketTextStream(host,1234)

#netcat is used to generate API
# $ nc -lk 1234
#lines.pprint()


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w:w.split(","))
        cols=["name","age","city"]
        df=rowRdd.toDF(["name","age","city"])
        df.show()

        hyddf=df.where(col("city")=="hyd")
        host="jdbc:mysql://mysqldb.cjyrchmwhfjw.us-east-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
        hyddf.write.mode("append").format("jdbc")\
            .option("url",host)\
            .option("user","myuser")\
            .option("password","mypassword")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("dbtable","livesparkstreaming").save()
    except:
        pass

lines.foreachRDD(process)

ssc.start()    #start the computation
ssc.awaitTermination()

#.ConnectException: Connection timed out: connect   .... its srcurity group issue
# if u get above error its security group issue add 0.0.0.0/0