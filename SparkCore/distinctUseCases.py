from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext


data="D:\\AVD\\SPARK\\datasets\\donations.csv"
drdd=sc.textFile(data)
ab='dt'
pro = drdd.filter(lambda x: ab not in x).map(lambda x: x.split(",")).map(lambda x: x[0]).distinct()

res=pro

for i in res.collect():
    print(i)
