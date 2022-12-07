from requests import get
import json


def only_base_and_quote(one):
    symb = one["symbol"]
    base = one["baseAsset"]
    quote = one["quoteAsset"]
    return {symb: [base, quote]}


def get_symbol():
    symbols_list = list(map(only_base_and_quote, get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]))
    symbols = {}
    for e in symbols_list:
        symbols.update(e)
    return symbols



# print(json.dumps(get("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": "ETHBTC"}).json(), indent=4))
with open("info.json", mode="w") as file:
    symbols_list = list(map(only_base_and_quote, get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]))
    symbols = {}
    for e in symbols_list:
        symbols.update(e)
    print(len(symbols))
    file.write(json.dumps(symbols, indent=4))