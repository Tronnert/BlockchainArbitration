import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
import pyspark.sql.functions as f
import sys
sys.path.append("../")
from functions import get_crypto_quotes


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


def price_usd(x):
    return quotes[x]


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
func_price = f.udf(price_usd, DoubleType())

df = spark.read.options(delimiter='\t', ).csv("../logs/to_find.tsv", header=False, schema=SCHEMA)
df = df.withColumn("idExchange", func_id("exchange"))

quotes = get_crypto_quotes([i["quote"] for i in df.select("quote").distinct().collect()])
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
test = test.withColumn("revenueUSD", f.col("revenue") * func_price("quote"))
test = test.withColumn("bidExchange", func_name("bidExchange")).withColumn("askExchange", func_name("askExchange"))

test3 = test.groupBy(["base", "quote", "bidExchange", "askExchange"]) \
    .agg(f.collect_list(f.struct("dt", "bidPrice", "askPrice", "Qty", "revenue")).alias("data"))

def calc_avg(x):
    return sum(x) / len(x)


def calc_len(x):
    return len(x) >= 1


def test_len(x):
    return len(x)


def find(rows):
    N = 10 ** 8
    if len(rows) == 1:
        return [N]
    arbitrages = []
    rows.sort(key=lambda x: x["dt"])
    old = rows[0]
    dur = 0
    for row in rows[1:]:
        if row["revenue"] > 0 and old["revenue"] > 0:
            dur += row["dt"] - old["dt"]
        elif row["revenue"] <= 0 and old["revenue"] > 0:
            arbitrages.append(dur + N)
            dur = 0
        old = row
    if row["revenue"] > 0:
        arbitrages.append(dur + N)
    return arbitrages


func = f.udf(find, ArrayType(LongType()))
func2 = f.udf(calc_avg, DoubleType())
func3 = f.udf(calc_len, BooleanType())
func4 = f.udf(test_len, IntegerType())

test3 = test3.withColumn("arbitrations", func("data")) \
    .withColumn("is_not_empty", func3("arbitrations")) \
    .withColumn("len", func4("arbitrations"))
test3 = test3[test3["is_not_empty"] == True].withColumn("avg_arb", func2("arbitrations"))

print(test3.sort("avg_arb", ascending=False).show())