import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
import pyspark.sql.functions as f


def get_id(x):
    """Устанавливает id для каждой биржи"""
    d = {"binance": 1.0, "poloniex": 2.0, "gate": 3.0, "huobi": 4.0,
         "kraken": 5.0, "bybit": 6.0, "bitget": 7.0}
    return d[x]


def get_name(x):
    """Взвращает биржу по ее id"""
    d = {1.0: 'binance', 2.0: 'poloniex', 3.0: 'gate', 4.0: 'huobi',
         5.0: 'kraken', 6.0: 'bybit', 7.0: 'bitget'}
    return d[x]


def array_len(x):
    return len(x)


def mult(rows):
    """Размножение данных (каждой записи с одной биржи ставятся все записи с
    других бирж)"""
    multed = []
    for row_bid in rows:
        for row_ask in rows:
            if row_bid["idExchange"] == row_ask["idExchange"]:
                continue
            multed.append([row_bid["idExchange"], row_ask["idExchange"],
                           row_bid["bidPrice"], row_bid["bidQty"],
                           row_ask["askPrice"], row_ask["askQty"],
                           row_bid["bidFee"], row_ask["askFee"],
                           row_bid["baseWithdrawalFee"]])
    return multed


spark = SparkSession.builder.appName('Crypto').getOrCreate()
SCHEMA = StructType([
    StructField("dt", LongType(), False),
    StructField("base", StringType(), False),
    StructField("quote", StringType(), False),
    StructField("baseWithdrawalFee", DoubleType(), False),
    StructField("exchange", StringType(), False),
    StructField("bidPrice", DoubleType(), False),
    StructField("bidQty", DoubleType(), False),
    StructField("bidFee", DoubleType(), False),
    StructField("askPrice", DoubleType(), False),
    StructField("askQty", DoubleType(), False),
    StructField("askFee", DoubleType(), False),
])

func_id = f.udf(get_id, DoubleType())
func_mult = f.udf(mult, ArrayType(ArrayType(DoubleType())))
func_len = f.udf(array_len, IntegerType())
func_name = f.udf(get_name, StringType())

df = spark.read.options(delimiter='\t', ).csv("../logs/to_find.tsv", header=False, schema=SCHEMA)
df = df.withColumn("idExchange", func_id("exchange"))

test = df.groupBy(['dt', "base", "quote"]).agg(f.collect_list(f.struct(
    "idExchange", "bidPrice", "bidQty", "askPrice", "askQty", "bidFee", "askFee", "baseWithdrawalFee"
)).alias("data")).withColumn("multed", func_mult("data")).withColumn("len", func_len("multed"))

test = test[test["len"] > 0]
test = test.select(test["dt"], test["base"], test["quote"], f.explode("multed"))
test = test.select(test["dt"], test["base"], test["quote"], *[f.col("col")[e] for e in range(9)])
test = test.withColumnRenamed("col[0]", "bidExchange")\
           .withColumnRenamed("col[1]", "askExchange")\
           .withColumnRenamed("col[2]", "bidPrice")\
           .withColumnRenamed("col[3]", "bidQty")\
           .withColumnRenamed("col[4]", "askPrice")\
           .withColumnRenamed("col[5]", "askQty")\
           .withColumnRenamed("col[6]", "bidFee")\
           .withColumnRenamed("col[7]", "askFee")\
           .withColumnRenamed("col[8]", "baseWithdrawalFee")

test = test.withColumn("Qty", f.least("bidQty", "askQty"))\
           .withColumn("revenue", (f.col("bidPrice") * (1 - f.col("bidFee") * (1 - f.col("baseWithDrawalFee")))
                                   - f.col("askPrice") / (1 - f.col("askFee"))) * f.col("Qty"))

test = test.withColumn("bidExchange", func_name("bidExchange")).withColumn("askExchange", func_name("askExchange"))
test.repartition(1).write.options(header='True', encoding="utf-8").csv("simple_revenue", sep="\t")