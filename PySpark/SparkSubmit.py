from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy('state').orderBy(col("zip").desc())
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("D:\\AVD\\SPARK\\datasets\\us-500.csv")
df=df.withColumn('ntile',ntile(10).over(win)).where(col('ntile')==1)
df.show()

df.createOrReplaceTempView('tab')
ndf2=spark.sql("select * from tab where zip between 85260 and 85381")
ndf2.show()

#spark-submit --master local --deploy-mode client


