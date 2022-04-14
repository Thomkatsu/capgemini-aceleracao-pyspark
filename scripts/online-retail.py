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

f = df.withColumn('Quantity', F.when(F.col('Quantity').isNull(), 0).when(F.col('Quantity') < 0, 0).otherwise(F.col('Quantity')))

# Adicionando a coluna 'TotalPrice' ao dataframe

df = df.withColumn('TotalPrice', F.col('Quantity')*F.col('UnitPrice'))
	
	
# Pergunta 1
# 1. Qual o valor de Gift Cards vendidos no total? Considere StockCode="gift_0001"

df_1 = df.filter(F.col('StockCode').startswith('gift_0001'))

df_1.agg(F.sum('TotalPrice')).show()

# Pergunta 2
# 2. Qual o valor total de Gift Cards vendidos por mês? Considere StockCode="gift_0001"

df_2 = df_1.groupBy(F.month(F.col('InvoiceDate'))).sum('TotalPrice').show()

# Pergunta 3
# 3. Qual o valor total de amostras que foram concedidas? Consider StockCode="S"

df_3 = df.filter(F.col('StockCode')=='S')

df_3.agg(F.sum('TotalPrice')).show()

# Pergunta 4
# 4. Qual o produto mais vendido?

df_4 = (df.groupBy(F.col('Description')).agg(F.sum('Quantity').alias('Quantity')).orderBy(F.col('Quantity').desc()).show(1))

# Pergunta 5
# 5. Qual o produto mais vendido por mês?

df_5 = (df.groupBy('Description', F.month('InvoiceDate').alias('Month'))
		    .agg(F.sum('Quantity').alias('Quantity'))
		    .orderBy(F.col('Quantity').desc())
		    .dropDuplicates(['Month'])
		    .show())

# Pergunta 6
# 6. Qual hora do dia tem maior valor de vendas?

df_6 = (df.groupBy(F.hour('InvoiceDate'))
	   .agg(F.round(F.sum(F.col('TotalPrice')), 2).alias('value'))
	   .orderBy(F.col('value').desc())
	   .show(1))

# Pergunta 7
# 7. Qual mês do ano tem maior valor de vendas?

df_7 = (df.groupBy(F.month('InvoiceDate'))
	   .agg(F.round(F.sum('TotalPrice'), 2).alias('TotalPrice'))
	   .orderBy(F.col('TotalPrice').desc())
	   .show(1))

# Pergunta 8
# 8. Qual o produto mais vendido no mês do ano tem maior valor de vendas?

