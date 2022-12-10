from requests import get


def get_top100pars():
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
    ticker24 = list(sorted(ticker24, key=lambda x: x["count"], reverse=True)[:100])
    ticker24 = list(map(lambda x: x | symbols[x["symbol"]], ticker24))
    top100pairs = dict()
    for pair in ticker24:
        top100pairs |= {pair["symbol"]: [pair["baseAsset"], pair["quoteAsset"]]}
    return top100pairs