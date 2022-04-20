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


# Tratamento UnitPrice

df = df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', ".").cast('float'))	

# Tratamento InvoiceDate

df = df.withColumn('InvoiceDate', F.concat(F.col('InvoiceDate'),F.lit(':00')))
df = df.withColumn("InvoiceDate", F.from_unixtime(F.unix_timestamp(F.col("InvoiceDate"),'d/M/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss').cast('timestamp'))  

# Tratamento Quantity

df = df.withColumn('Quantity', F.when(F.col('Quantity').isNull(), 0).when(F.col('Quantity') < 0, 0).otherwise(F.col('Quantity')))

# Adicionando a coluna 'TotalPrice' ao dataframe

df = df.withColumn('TotalPrice', F.col('Quantity')*F.col('UnitPrice'))
	
	
# Pergunta 1

df_1 = df.filter(F.col('StockCode').startswith('gift_0001'))

df_1.agg(F.sum('TotalPrice')).show()

# Pergunta 2

df_2 = df_1.groupBy(F.month(F.col('InvoiceDate'))).sum('TotalPrice').show()

# Pergunta 3

df_3 = df.filter(F.col('StockCode')=='S')

df_3.agg(F.sum('TotalPrice')).show()

# Pergunta 4

df_4 = (df.groupBy(F.col('Description')).agg(F.sum('Quantity').alias('Quantity')).orderBy(F.col('Quantity').desc()).show(1))

# Pergunta 5

df_5 = (df.groupBy('Description', F.month('InvoiceDate').alias('Month'))
		    .agg(F.sum('Quantity').alias('Quantity'))
		    .orderBy(F.col('Quantity').desc())
		    .dropDuplicates(['Month'])
		    .show())

# Pergunta 6

df_6 = (df.groupBy(F.hour('InvoiceDate'))
	   .agg(F.round(F.sum(F.col('TotalPrice')), 2).alias('value'))
	   .orderBy(F.col('value').desc())
	   .show(1))

# Pergunta 7

df_7 = (df.groupBy(F.month('InvoiceDate'))
	   .agg(F.round(F.sum('TotalPrice'), 2).alias('TotalPrice'))
	   .orderBy(F.col('TotalPrice').desc())
	   .show(1))


def pergunta_8(df):
	sales_per_month = (df.groupBy(F.month('InvoiceDate'), "StockCode")
						 .agg({'Sale': 'sum'})
						 .withColumnRenamed('sum(Sale)', 'Sale')
						 .withColumnRenamed('month(InvoiceDate)', 'month'))

	windowsSpec      = Window.partitionBy('month').orderBy(F.col('Sale').desc())
	sales_per_month2 = sales_per_month.withColumn("row_number", F.row_number().over(windowsSpec))
	return sales_per_month2.filter(sales_per_month2.row_number == 1).show()

def pergunta_9(df):
	return (df.groupBy("Country")
	  	   .agg({'Sale':'count'})
	  	   .withColumnRenamed('count(Sale)', 'Sale')
	       .sort('Sale', ascending = False).show()
	)

def pergunta_10(df):
	manual = df.filter((F.col('StockCode').startswith('M')) & (F.col('StockCode') != 'PADS') & (F.col('Quantity') < 0))
	return (manual.groupBy("Country")
	  	   .agg({'Sale':'count'})
	  	   .withColumnRenamed('count(Sale)', 'Sale')
	       .sort('Sale', ascending = False).show()
	)

def pergunta_11(df):
	return (df.groupBy('InvoiceNo')
			  .agg({'UnitPrice': 'max'})
			  .withColumnRenamed('max(UnitPrice)', 'UnitPrice')
			  .sort('UnitPrice', ascending = False)
			  .show()
	)

def pergunta_12(df):
	return (df.groupBy('InvoiceNo')
			.agg({'UnitPrice': 'count'})
			.withColumnRenamed('count(UnitPrice)', 'Count')
			.sort('Count', ascending = False)
			.show()
	)

def pergunta_13(df):
	customerID_not_null = df.filter(F.col('CustomerID').isNotNull())
	return (customerID_not_null.groupBy('CustomerID')
							   .agg({'UnitPrice': 'count'})
							   .withColumnRenamed('count(UnitPrice)', 'Count')
							   .sort('Count', ascending = False)
							   .show()
	)