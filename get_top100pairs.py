from requests import get
import json


def get_top100pairs_poloniex() -> dict:
    ticker24 = get("https://api.poloniex.com/markets/ticker24h").json()
    # ticker24 = list(sorted(ticker24, key=lambda x: x["tradeCount"], reverse=True)[:20])
    top100pairs_poloniex = dict()
    for pair in ticker24:
        top100pairs_poloniex |= {"".join(pair["symbol"].split("_")): pair["symbol"].split("_")}
    return top100pairs_poloniex


def get_top100pairs_binance() -> dict:
    def only_base_and_quote(one):
        symb = one["symbol"]
        base = one["baseAsset"]
        quote = one["quoteAsset"]
        return {symb: {"baseAsset": base, "quoteAsset": quote}}

    symbols_list = list(map(only_base_and_quote, get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]))
    symbols = dict()
    for e in symbols_list:
        symbols |= e
    ticker24 = get("https://api.binance.com/api/v3/ticker/24hr").json()
    # ticker24 = list(sorted(ticker24, key=lambda x: x["count"], reverse=True)[:100])
    ticker24 = list(map(lambda x: x | symbols[x["symbol"]], ticker24))
    top100pairs_binance = dict()
    for pair in ticker24:
        top100pairs_binance |= {pair["symbol"]: [pair["baseAsset"], pair["quoteAsset"]]}
    return top100pairs_binance

# def get_top100pairs_kraken() -> dict:
#     ticker24 = get("https://api.kraken.com/0/public/Ticker").json()["result"]
#     # ticker24 = list(sorted(ticker24.values(), key=lambda x: x["t"], reverse=True)[:20])
#     top100pairs_poloniex = dict()
#     for pair in ticker24:
#         top100pairs_poloniex |= {"".join(pair["symbol"].split("_")): pair["symbol"].split("_")}
#     return top100pairs_poloniex

def kraken() -> dict:
    resp = get('https://api.kraken.com/0/public/AssetPairs').json()["result"]
    fuck = json.load(open("different_names.json"))
    ans = dict()
    for val in resp.values():
        val = list(map(lambda x: fuck.get(x, x), val["wsname"].split("/")))
        
        ans |= {"".join(val): val}
    return ans

binance = get_top100pairs_binance()
poloniex = get_top100pairs_poloniex()
krak = kraken()
print(binance)
print(poloniex)
print(krak)
print(len(binance))
print(len(poloniex))
print(len(krak))
print("ETHUSDT" in binance.keys())
print("ETHUSDT" in poloniex.keys())
print("ETHUSDT" in krak.keys())
a = set(binance.keys()) & set(poloniex.keys())
b = set(binance.keys()) & set(krak.keys())
c = set(krak.keys()) & set(poloniex.keys())
print("ETHUSDT" in set(binance.keys()))
print("ETHUSDT" in a)
print(a)
print(len(a))
print(len(b))
print(len(c))