import pandas as pd
import sys
sys.path.append("../")
from functions import draw_symb_or_exchange, draw_sub, draw_based_on_dt

df = pd.read_csv("revenue/part-00000.csv", sep='\t')
df["symbol"] = df["base"] + "_" + df["quote"]

# ТОП-10 пар символов для каждой пары бирж по средней арбитражной паре
most_rev = df.groupby(["bidExchange", "askExchange", "base", "quote"])["revenueUSD"].mean().reset_index()
most_rev = most_rev.groupby(["bidExchange", "askExchange"]).apply(lambda x: x.nlargest(5, ["revenueUSD"])).reset_index(drop=True)\
    .drop(columns=["revenueUSD"])

test = most_rev.merge(df, on=["bidExchange", "askExchange", "base", "quote"], how="inner")

# ТОП-10 символов по средней прибыли
top_symbols = df.groupby(["base", "quote", "symbol"])["revenueUSD"].mean().nlargest(5).reset_index().drop(columns=["revenueUSD"])
test2 = top_symbols.merge(df, on=["base", "quote", "symbol"], how="inner")
symbol_names = top_symbols["symbol"].tolist()

exchanges = ["binance", "bitget", "bybit", "gate", "huobi", "kraken", "poloniex"]
for exch in exchanges:
    draw_symb_or_exchange(test, exch, "exchange", size=(40, 25), pad=5, rows=2, exclude_loss=True).savefig(f"{exch}_only_profitable.png")
    draw_symb_or_exchange(test, exch, "exchange", size=(40, 25), pad=5, rows=2, exclude_loss=False).savefig(f"{exch}_all.png")

for symb in symbol_names:
    draw_symb_or_exchange(test2, symb, "symbol", size=(40, 25), pad=5, rows=2, exclude_loss=True).savefig(f"{symb}_only_profitable.png")
    draw_symb_or_exchange(test2, symb, "symbol", size=(40, 25), pad=5, rows=2, exclude_loss=False).savefig(f"{symb}_all.png")