from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
# data=[12,32,34,4,54,26]
# drdd =spark.sparkContext.parallelize(data)
data = "D:\\AVD\\SPARK\\drivers\\asl.csv"
aslrdd = sc.textFile(data)

#res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:"blr" in x[2])

#res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:x[0]=="venu")

res=aslrdd.filter(lambda x:"age" not in x).map(lambda x: x.split(",")).filter(lambda x:int(x[1])>=30)

#Filter by default apply a logic/filter on top of the entire line

#Filter almost in SQL you are using where condition to filter results similarly u r using filter function

for i in res.collect():
    print(i)