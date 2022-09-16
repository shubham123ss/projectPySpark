from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
# data=[12,32,34,4,54,26]
# drdd =spark.sparkContext.parallelize(data)
data = "D:\\AVD\\SPARK\\drivers\\asl.csv"
aslrdd = sc.textFile(data)

# res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:"blr" in x[2])

# res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:x[0]=="venu")

res=aslrdd.filter(lambda x:"age" not in x).map(lambda x: x.split(",")).toDF(["name","age","city"])
res.createOrReplaceTempView("tab") #DataFrame you are giving one name .. it is used to run SQL queries on top of DataFrame
# result=spark.sql("select * from tab where city='blr' and age<=30")
# result=res.where(col("age")>=30)
result=res.where((col("age")>=30) & (col("city")=="mas"))
result.show()
#above steps mostly used in 2015 after march .. above approch ... RDD you are converting to DataFrame.
# but after 2016 mostly use dataframe api
df=spark.read