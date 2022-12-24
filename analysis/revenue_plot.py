import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_csv("simple_revenue.tsv", sep="\t")

gbr = df.groupby(["base", "quote", "bidExchange", "askExchange"]).agg({"revenue": "mean"}).sort_values(by="revenue", ascending=False)
print(gbr)