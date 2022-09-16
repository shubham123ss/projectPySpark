from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
# data=[12,32,34,4,54,26]
# drdd =spark.sparkContext.parallelize(data)
data = "D:\\AVD\\SPARK\\drivers\\asl.csv"
aslrdd = sc.textFile(data)

'''select * from tab where city="hyd"'''
#res=aslrdd.filter(lambda x: 'age' not in x).map(lambda x: x.split(",")).filter(lambda x: 'hyd' in x)

'''
group by you are using is done on catageroy based column and someting aggregation is required/ mandatory

if you want to group the value you must use reduceByKey. why it use to group the values
--reduceByKey (any function/method ends with Key, means data ,must be key value format
==> reduceByKey does based on same key, data process the values.
reduceByKey takes internally two values ==> (x,y: x+y) 
'''
res=aslrdd.filter(lambda x: 'age' not in x).map(lambda x: x.split(",")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)



'''select * from tab where city="blr"'''
#res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:x[2]=='blr')

'''select * from tab where name="venu"'''
#res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:x[0]=="venu")

'''"select * from tab where age >=30"'''
# res=aslrdd.filter(lambda x:"age" not in x).map(lambda x: x.split(",")).filter(lambda x:int(x[1])>=30)

#Filter by default apply a logic/filter on top of the entire line

#Filter almost in SQL you are using where condition to filter results similarly u r using filter function

for i in res.collect():
    print(i)