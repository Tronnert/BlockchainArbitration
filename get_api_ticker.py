from requests import get
import json


def only_hot(one):
    return one["count"] >= 10000


def only_base_and_quote(one):
    symb = one["symbol"]
    base = one["baseAsset"]
    quote = one["quoteAsset"]
    return {symb: {"baseAsset": base, "quoteAsset": quote}}


def get_symbol():
    symbols_list = list(map(only_base_and_quote, get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]))
    symbols = {}
    for e in symbols_list:
        symbols.update(e)
    return symbols


def symbol_to_pair(one):
    one.update(symbols[one["symbol"]])
    return one


# print(json.dumps(get("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": "ETHBTC"}).json(), indent=4))
with open("24.json", mode="w") as file:
    symbols = get_symbol()
    answer = list(map(symbol_to_pair, filter(only_hot, get("https://api.binance.com/api/v3/ticker/24hr").json())))
    print(len(answer))
    file.write(json.dumps(answer, indent=4))