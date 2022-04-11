from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema_online_retail = StructType([
	StructField('InvoiceNo', StringType(), True),
	StructField('StockCode', StringType(), True),
	StructField('Description', StringType(), True),
	StructField('Quantity', IntegerType(), True),
	StructField('InvoiceDate', StringType(), True),
	StructField('UnitPrice', StringType(), True),
	StructField('CustomerID', IntegerType(), True),
	StructField('Country', StringType(), True),
])
 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	
# Pergunta 1
# 1. Qual o valor de Gift Cards vendidos no total? Considere StockCode="gift_0001"

df = df.withColumn('UnitPrice', F.regexp_replace('UnitPrice', ',', '.'))

df = df.withColumn('UnitPrice', F.col('UnitPrice').cast("float"))

df = df.withColumn('TotalPrice', F.col('Quantity')*F.col('UnitPrice'))

# df.printSchema()

# df.show()

df.agg(F.sum('TotalPrice')).show()