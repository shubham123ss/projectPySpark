from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

# File location and type
data = "/FileStore/tables/us_500.csv"
df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(data)
df.show()


from pyspark.sql.functions import *
res=df.withColumn("today", current_date()).drop("phone1","phone2","email","web","address").withColumnRenamed("zip","sal")
# withColumnRenamed (old_column,new_column) used to remane one column at a time.
# if you want to raname all columns use toDF()
res.show()


res.createOrReplaceTempView("tab")
result=spark.sql(" select max(sal),state from tab group by state order by state")
result.show()

res.createOrReplaceTempView("tab")
result = res.orderBy(col("sal").desc()).withColumn("rno", monotonically_increasing_id() + 1).where(col("rno") <= 5)
# monotonically_increasing_id() ... gets unique numbers starting from 0
# =======WINDOWS FUNCTION======
result.show()



from pyspark.sql.window import Window
from pyspark.sql.functions import *
# partitionBy() used always catagory based columns
win = Window.partitionBy("state").orderBy(col("sal").desc())
fin = res.drop("county", "company_name", "city").withColumn("rnk", rank().over(win)).withColumn("drnk",dense_rank().over(win)).withColumn("row", row_number().over(win)).withColumn("prank", percent_rank().over(win)).withColumn("ntr", ntile(4).over(win)).withColumn("lead", lead(col("sal")).over(win)).withColumn("lag", lag("sal").over(win)).na.fill(0).withColumn("diff", col("sal") - col("lead")).withColumn("fst", first(col("sal")).over(win))
fin.show(109)


# Create a view or table
temp_table_name = "us_500_csv"
df.createOrReplaceTempView(temp_table_name)

'''
% sql
#Query the created temp table in a SQL cell * /
select *from`us_500_csv`'''


# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.
permanent_table_name = "us_500_csv"
# df.write.format("parquet").saveAsTable(permanent_table_name)