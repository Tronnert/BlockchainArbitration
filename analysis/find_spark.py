from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, DoubleType, BooleanType
import pyspark.sql.functions as f
from pyspark.sql import Window
import findspark

findspark.init()
spark = SparkSession.builder.appName('Crypto').getOrCreate()
SCHEMA = StructType([
    StructField("dt", DoubleType(), False),
    StructField("base", StringType(), False),
    StructField("quote", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("bidPrice", DoubleType(), False),
    StructField("bidQty", DoubleType(), False),
    StructField("askPrice", DoubleType(), False),
    StructField("askQty", DoubleType(), False),
    
])
df = spark.read.options(delimiter='\t', ).csv("../logs/logs9.tsv", header=False, schema=SCHEMA)

w = Window.partitionBy(['dt', "base", "quote"])
bids = df.withColumn('maxBid', f.max('bidPrice').over(w))\
    .where(f.col('bidPrice') == f.col('maxBid'))\
    .drop('maxBid').withColumnRenamed("exchange", "bidExchange") \
    .drop("askPrice").drop("askQty")
asks = df.withColumn('minAsk', f.min('askPrice').over(w))\
    .where(f.col('askPrice') == f.col('minAsk'))\
    .drop('minAsk').withColumnRenamed("exchange", "askExchange") \
    .drop("bidPrice").drop("bidQty")
test = bids.join(asks, on=["dt", "base", "quote"]) \
    .withColumn("Qty", f.least("bidQty", "askQty")) \
    .withColumn("revenue", (f.col("bidPrice") - f.col("askPrice")) * f.col("Qty"))
test = test[test["revenue"] > 0]

test3 = test.groupBy(["base", "quote", "bidExchange", "askExchange"]) \
    .agg(f.collect_list(f.struct("dt", "bidPrice", "askPrice", "Qty")).alias("data"))


def calc_avg(x):
    return sum(x) / len(x)


def get_values(row):
    return row["dt"], row["bidPrice"], row["askPrice"], row["Qty"]


def find(rows):
    N = 0.1
    if len(rows) == 1:
        return [N]
    arbitrages = []
    rows.sort(key=lambda x: x["dt"])
    old_dt = rows[0]["dt"]
    start = old_dt
    for row in rows[1:]:
        new_dt = row["dt"]
        if new_dt - old_dt > N:
            arbitrages.append(old_dt - start + N)
            start = new_dt
        old_dt = new_dt
    arbitrages.append(old_dt - start + N)
    return arbitrages


func = f.udf(find, ArrayType(DoubleType()))
func2 = f.udf(calc_avg, DoubleType())
test3 = test3.withColumn("arbitrations", func("data"))
test3 = test3.withColumn("avg_arb", func2("arbitrations"))
print(test3.sort("avg_arb", ascending=False).show())