from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as f
from pyspark.sql import Window
import findspark

findspark.init()
spark = SparkSession.builder.appName('Crypto').getOrCreate()
SCHEMA = StructType([
    StructField("dt", LongType(), False),
    StructField("base", StringType(), False),
    StructField("quote", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("bidPrice", DoubleType(), False),
    StructField("bidQty", DoubleType(), False),
    StructField("askPrice", DoubleType(), False),
    StructField("askQty", DoubleType(), False),

])
df = spark.read.options(delimiter='\t', ).csv("../logs9.tsv", header=False,
                                              schema=SCHEMA)

w = Window.partitionBy(['dt', "base", "quote"])
bids = df.withColumn('maxBid', f.max('bidPrice').over(w))\
    .where(f.col('bidPrice') == f.col('maxBid'))\
    .drop('maxBid').withColumnRenamed("exchange", "bidExchange")
bids = bids.select(bids.schema.names[:-2])
asks = df.withColumn('minAsk', f.min('askPrice').over(w))\
    .where(f.col('askPrice') == f.col('minAsk'))\
    .drop('minAsk').withColumnRenamed("exchange", "askExchange")
asks = asks.select(asks.schema.names[:-4] + asks.schema.names[-2:])
test = bids.join(asks, on=["dt", "base", "quote"])
test = test.withColumn("Qty", f.least("bidQty", "askQty"))
test = test.withColumn("revenue", (f.col("bidPrice") - f.col("askPrice")) * f.col("Qty"))
test2 = test[test["revenue"] > 0]
print(test2.count())
print(df.count())