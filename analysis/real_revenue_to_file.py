import pandas as pd

df = pd.read_csv("revenue_usd/revenue.csv", sep="\t")


def func(group):
    old_prices = {}
    base = {"bid": -1, "ask": -1}
    revenue = [0]
    def sec(singe_dt):
        singe_dt = singe_dt.sort_values("revenue", ascending=False)
        def thi(now):
            if old_prices.get(now["bidExchange"], base)["bid"] != now["bidPrice"] and \
            old_prices.get(now["askExchange"], base)["ask"] != now["askPrice"]:
                old_prices[now["bidExchange"]] = {"bid": now["bidPrice"], "ask": old_prices.get(now["bidExchange"], base)["ask"]}
                old_prices[now["askExchange"]] = {"bid": old_prices.get(now["askExchange"], base)["ask"], "ask": now["askPrice"]}
                if now["revenueUSD"] > 0:
                    revenue[0] += now["revenueUSD"]
        singe_dt.apply(thi, axis=1)
    group.groupby("dt").apply(sec)
    return revenue[0]

grb = df.groupby(["base", "quote"], group_keys=True).apply(func)                      
grb.to_csv("real_revenue.tsv", sep="\t")