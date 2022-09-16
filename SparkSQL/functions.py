from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="D:\\AVD\\SPARK\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",",").load(data)
#ndf=df.groupby(df.state).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
#ndf=df

#ndf=df.groupby(df.state).agg(count("*").alias("cnt"), collect_set(df.city).alias("city")).orderBy(col("cnt").desc())
''' 
in collect_set()==> the duplicate elements are eleminated
in collect_list()==> the list is shown with duplicates
'''
#ndf=df.groupby(df.state).agg(count("*").alias("cnt"), collect_list(df.city).alias("city")).orderBy(col("cnt").desc())
'''Withcolumn used to add new column (if column not exits) or upload (if already column exist)
    lit(value) use to add something dummy value
    --drop (columns) ... delete unnecessary columns
    --withColumnRenamed() use to rename one column at a time '''
#ndf=df.withColumn("age",lit(18)).withColumn("phone1",lit(999999999))
#ndf=df.withColumn("fullname", concat_ws("_",df.first_name, df.last_name, df.state)).withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType())).withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType( ))).drop("email","city","country","address","web").withColumnRenamed("first_name","fname").withColumnRenamed("last_name","lname")

#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","CaliFornia").otherwise(col("state")))
#ndf=df.withColumn("address1",when(col("address").contains("#"),"****").otherwise(col("address"))).withColumn("address2",regexp_replace(col("address"),"#","_"))
#ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("username",substring_index(col("email"),"@",1)).withColumn("mail",substring_index(col("email"),"@",-1))
#ndf=ndf1.groupby(col("mail")).count().orderBy(col("count").desc())
df.createOrReplaceTempView("tab")
#ndf=spark.sql("select *,substring(email,0,instr(email,'@')-1) as username,substring(email,instr(email,'@')) from tab",)
#ndf=spark.sql("select *,concat_ws('_',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab")
'''using SQL query to run directly in sqprk.SQL'''

#qry="""select *,concat_ws('_',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab"""
qry="""with tmp as (select *,concat_ws('_',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab)
        select mail,count(*) cnt from tmp group by mail order by cnt desc
        """
#ndf=spark.sql(qry)


"""
=======>>>>> CREATING YOUR OWN FUNCTION

-- by default spark unable to understand python functions. so convert python/java/scala functions to UDF (spark able to understand UDF)
  
"""
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500% off"
uf=udf(func)
#ndf=df.withColumn("offer",uf(col("state")))
spark.udf.register("offer",uf) #=======>>>>> User Defined Function UDF converting to SQL function
ndf=spark.sql("select *,offer(state) TodaysOffer from tab")
ndf.printSchema()
ndf.show(truncate=False)