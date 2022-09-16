from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\AVD\SPARK\drivers\emailsmay4.txt"
erdd = spark.sparkContext.textFile(data)
# res = erdd.filter(lambda x: '@' in x).map(lambda x: x.split(" ")).map(lambda x: (x[0], x[-1]))
res = erdd.filter(lambda x: '@' not in x).map(lambda x: x.split(" ")).map(lambda x: (x[0], x[-1])) \
    .filter(lambda x: ('Venu' in x) or ('sir' in x))

for i in res.collect():
    print(i)
