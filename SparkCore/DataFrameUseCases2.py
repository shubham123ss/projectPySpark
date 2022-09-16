from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="D:\\AVD\\SPARK\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",";").load(data)

'''
---now to Data Processing Programming Friendly
        it is like SQL, where we can process the data using different df 
'''
#res=df.where((col("age")>60) & (col("marital")!="married"))

'''
--To display only selected column use .select(col("COL-NAME0"),col("COL-NAME1"),col("COL-NAME2")) 
'''
#res=df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) & (col("marital")!="married"))
#res=df.where((col("age")>60) & (col("marital")!="married") & (col("balance")>=40000))
''' "or" condition can be archived by using "|" symbol '''
res=df.where(((col("age")>60) | (col("marital")!="married")) & (col("balance")>=40000))
#res=df.groupby(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
#res=df.groupby(col("marital")).count()
#res=df.groupby(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb")).having(col("balance")>avg(col("balance"))).orderBy(col("smb").desc())
#res=df.where((col("balance"))>(sum(col("balance"))/count("*")))

'''Process SQL friendly'''
#df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where age>60 and balance>50000")
#res=spark.sql("select count(*) from tab")
#res=spark.sql("select marital,sum(balance) from tab group by marital ")
#res.printSchema()
res.show()

