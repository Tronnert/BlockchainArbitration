from consts import BINANCE_SUB_FILE, BINANCE_STREAM_NAME
from base_websocket import BaseWebsocket
from requests import get

class BinanceWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(BINANCE_SUB_FILE, BINANCE_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(300)

    def made_sub_json(self) -> None:
        sub_json = super().made_sub_json()
        ans = []
        for e in range(0, len(self.list_of_symbols), 50):
            sub_json["params"] = list(map(lambda x: x.lower() + "@bookTicker", list(self.list_of_symbols.keys())[e: e + 50]))
            ans.append(sub_json.copy())
        return ans

    def get_top_pairs(self, top: int) -> dict:
        symbols_list = get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]
        symbols = dict()
        for e in symbols_list:
            symbols[e["symbol"]] = e
        ticker24 = get("https://api.binance.com/api/v3/ticker/24hr").json()
        ticker24 = list(sorted(ticker24, key=lambda x: x["count"], reverse=True)[:top])
        ticker24 = list(map(lambda x: x | symbols[x["symbol"]], ticker24))
        top_pairs_binance = dict()
        for pair in ticker24:
            top_pairs_binance |= {pair["symbol"]: [pair["baseAsset"], pair["quoteAsset"]]}
        return top_pairs_binance

    def on_message(self, ws, mess):
        mess = super().on_message(ws, mess)
        self.resent[mess["s"]] = (*self.list_of_symbols[mess["s"]], "binance", float(mess["b"]), float(mess["a"]))
