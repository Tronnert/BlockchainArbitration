from consts import POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME 
from base_websocket import BaseWebsocket
from requests import get

class PoloniexWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top100pairs_poloniex(300)

    def get_top100pairs_poloniex(self, top) -> dict:
        ticker24 = get("https://api.poloniex.com/markets/ticker24h").json()
        ticker24 = list(sorted(ticker24, key=lambda x: x["tradeCount"], reverse=True)[:top])
        top100pairs_poloniex = dict()
        for pair in ticker24:
            top100pairs_poloniex |= {"".join(pair["symbol"].split("_")): pair["symbol"].split("_")}
        return top100pairs_poloniex


    def made_sub_json(self) -> None:
        sub_json = super().made_sub_json()
        sub_json["symbols"] = list(map(lambda x: "_".join(x), self.list_of_symbols.values()))
        return sub_json

    def on_message(self, ws, mess) -> None:
        mess = super().on_message(ws, mess)
        cur1, cur2 = mess["data"][0]["symbol"].split('_')
        if cur1 in self.different_names.keys():
            cur1 = self.different_names[cur1]
        if cur2 in self.different_names.keys():
            cur2 = self.different_names[cur2]
        self.resent[mess["data"][0]["symbol"]] = (cur1, cur2, "poloniex", float(mess["data"][0]["bids"][0][0]), \
                                                                          float(mess["data"][0]["bids"][0][1]), \
                                                                          float(mess["data"][0]["asks"][0][0]), \
                                                                          float(mess["data"][0]["asks"][0][1]))
