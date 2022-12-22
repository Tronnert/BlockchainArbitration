import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
import pyspark.sql.functions as f
from pyspark.sql import Window
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName('Crypto').getOrCreate()
SCHEMA = StructType([
    StructField("dt", LongType(), False),
    StructField("base", StringType(), False),
    StructField("quote", StringType(), False),
    StructField("baseWithdrawalFee", DoubleType(), False),
    StructField("baseWithdrawalFeeType", StringType(), False),
    StructField("quoteWithdrawalFee", DoubleType(), False),
    StructField("quoteWithdrawalFeeType", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("bidPrice", DoubleType(), False),
    StructField("bidQty", DoubleType(), False),
    StructField("bidFee", DoubleType(), False),
    StructField("askPrice", DoubleType(), False),
    StructField("askQty", DoubleType(), False),
    StructField("askFee", DoubleType(), False),
])

df = spark.read.options(delimiter='\t', ).csv("../logs/logs_2328.tsv", header=False, schema=SCHEMA)

def get_id(x):
    d = {"binance": 1.0, "poloniex": 2.0, "gate": 3.0, "huobi": 4.0, "kraken": 5.0, "bybit": 6.0, "bitget": 7.0}
    return d[x]

func_id = f.udf(get_id, DoubleType())
df = df.withColumn("idExchange", func_id("exchange"))

test = df.groupBy(['dt', "base", "quote"])\
         .agg(f.collect_list(f.struct("idExchange", "bidPrice", "bidQty", "askPrice", "askQty", "bidFee", "askFee")).alias("data"))

def mult(rows):
    multed = []
    for row_bid in rows:
        for row_ask in rows:
            if row_bid["idExchange"] != row_ask["idExchange"]:
                multed.append([row_bid["idExchange"], 
                               row_ask["idExchange"], 
                               row_bid["bidPrice"], 
                               row_bid["bidQty"], 
                               row_ask["askPrice"], 
                               row_ask["askQty"],
                               row_bid["bidFee"],
                               row_ask["askFee"]])
    return multed

func_mult = f.udf(mult, ArrayType(ArrayType(DoubleType())))

test = test.withColumn("multed", func_mult("data"))

def pyspark_len(x):
    return len(x)

func_len = f.udf(pyspark_len, IntegerType())

test = test.withColumn("len", func_len("multed"))

test = test[test["len"] > 0]

test = test.select(test["dt"], test["base"], test["quote"], f.explode("multed"))

test = test.select(test["dt"], test["base"], test["quote"], *[f.col("col")[e] for e in range(8)])

test = test.withColumnRenamed("col[0]", "bidExchange")\
           .withColumnRenamed("col[1]", "askExchange")\
           .withColumnRenamed("col[2]", "bidPrice")\
           .withColumnRenamed("col[3]", "bidQty")\
           .withColumnRenamed("col[4]", "askPrice")\
           .withColumnRenamed("col[5]", "askQty")\
           .withColumnRenamed("col[6]", "bidFee")\
           .withColumnRenamed("col[7]", "askFee")

test = test.withColumn("Qty", f.least("bidQty", "askQty"))\
           .withColumn("revenue", (f.col("bidPrice") * (1 - f.col("bidFee")) - f.col("askPrice") / (1 - f.col("askFee"))) * f.col("Qty"))

def get_name(x):
    d = {1.0: 'binance', 2.0: 'poloniex', 3.0: 'gate', 4.0: 'huobi', 5.0: 'kraken', 6.0: 'bybit', 7.0: 'bitget'}
    return d[x]

func_name = f.udf(get_name, StringType())
test = test.withColumn("bidExchange", func_name("bidExchange")).withColumn("askExchange", func_name("askExchange"))

test.write.options(header='True', encoding="utf-8").csv("simple_revenue.tsv", sep="\t")