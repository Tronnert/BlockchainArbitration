import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("revenue_usd/revenue.csv", sep="\t")


def func(group):
    old_prices = {}
    base = {"bid": -1, "ask": -1}
    revenue = [0]
    def sec(singe_dt):
        singe_dt = singe_dt.sort_values("revenue", ascending=False)
        def thi(now):
            revenue = 0
            if old_prices.get(now["bidExchange"], base)["bid"] != now["bidPrice"] and \
            old_prices.get(now["askExchange"], base)["ask"] != now["askPrice"]:
                old_prices[now["bidExchange"]] = {"bid": now["bidPrice"], "ask": old_prices.get(now["bidExchange"], base)["ask"]}
                old_prices[now["askExchange"]] = {"bid": old_prices.get(now["askExchange"], base)["ask"], "ask": now["askPrice"]}
                if now["revenueUSD"] > 0:
                    revenue += now["revenueUSD"]
            return revenue
        return singe_dt.apply(thi, axis=1).agg("sum")
    return group.groupby("dt", group_keys=True).apply(sec)
    # return revenue[0]

df.groupby(["base", "quote"], group_keys=True).apply(func).groupby("dt").agg("sum").cumsum().plot()
plt.show()