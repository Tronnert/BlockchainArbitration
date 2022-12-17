from consts import KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME
from sockets.base_websocket import BaseWebsocket
from requests import get


class KrakenWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME, *args)
        self.list_of_symbols = self.krakelist()

    def krakelist(self) -> dict:
        resp = get('https://api.kraken.com/0/public/AssetPairs').json()["result"]
        ans = dict()
        for val in resp.values():
            val = list(map(lambda x: self.different_names.get(x, x), val["wsname"].split("/")))
            ans |= {"".join(val): val}
        return ans

    def made_sub_json(self) -> None:
        sub_json = super().made_sub_json()
        sub_json["pair"] = list(map(lambda x: "/".join(x), self.list_of_symbols.values()))
        return sub_json

    def on_message(self, ws, mess):
        mess = super().on_message(ws, mess)
        cur1, cur2 = map(lambda x: self.different_names.get(x, x), mess[3].split('/'))
        symb = cur1 + cur2
        if 'as' in mess[1].keys():
            bids, asks = mess[1]["bs"], mess[1]["as"]
            if not bids:
                bids = [[0, 0]]
            if not asks:
                asks = [[0, 0]]
            self.resent[symb] = (cur1, cur2, "kraken", float(bids[0][0]), float(bids[0][1]), float(asks[0][0]), float(asks[0][1]))
        elif 'a' in mess[1].keys():
            asks = mess[1]["a"]
            if not asks:
                asks = [[0, 0]]
            self.resent[symb] = (*self.resent[mess[3]][:-1], float(asks[0][0]), float(asks[0][1]))
        elif 'b' in mess[1].keys():
            bids = mess[1]["b"]
            if not bids:
                bids = [[0, 0]]
            self.resent[symb] = (*self.resent[mess[3]][:-2], float(bids[0][0]), float(bids[0][1]), self.resent[mess[3]][-1])