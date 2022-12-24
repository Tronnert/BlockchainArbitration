import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_csv("real_revenue.tsv", sep="\t")
df = df.rename(columns={"0": "revenue real"}).sort_values("revenue real", ascending=False).iloc[:10]
df["symb"] = df["base"] + df["quote"]
df.plot.bar(y="revenue real", x="symb")
plt.show()