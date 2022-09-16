from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

#DataFrame Syntax
#data = "D:\\AVD\\SPARK\\datasets\\donations.csv"
#df=spark.read.format("csv").option("header","True").load(data)
'''If you mention header as True it will consider fist line as column names

** but what if your data starts from 3rd line==> above syntav gives wrong table format'''
data = "D:\\AVD\\SPARK\\datasets\\donations1.csv"
#df=spark.read.format("csv").option("header","True").load(data)

'''
--so in that case insted of reading data like this 
--create an RDD of the data 
skip=rdd.first() --to skip the first line
Orig_data=rdd.filter(lambda x: x!=skip)
-- then create an DF using that RDD 
df=spark.read.csv(Orig_data,header=True) 
'''
rdd=sc.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True) #inferSchema is use to automatically convert the data into there approprete datatypes
df.printSchema()
#df.show() # by default it will display top 20 row
df.show(5)