from consts import KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME, KRAKEN_SYMBOLS
from sockets.base_websocket import BaseWebsocket
from requests import get


class KrakenWebsocket(BaseWebsocket):
    """Сокет для Kraken'а"""
    def __init__(self, *args) -> None:
        super().__init__(KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs()

    def get_top_pairs(self) -> dict:
        resp = get(KRAKEN_SYMBOLS).json()["result"]
        ans = dict()
        for val in resp.values():
            val = self.rename(val["wsname"].split("/"))
            ans["".join(val)] = val
        return ans

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        sub_json["pair"] = list(map(lambda x: "/".join(x), self.list_of_symbols.values()))
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        cur1, cur2 = map(lambda x: self.different_names.get(x, x),
                         message[3].split('/'))
        symb = cur1 + cur2
        if 'as' in message[1].keys():
            bids, asks = message[1]["bs"], message[1]["as"]
            if not bids:
                bids = [[0, 0]]
            if not asks:
                asks = [[0, 0]]
            self.resent[symb] = (
                cur1, cur2, "kraken", float(bids[0][0]), float(bids[0][1]),
                float(asks[0][0]), float(asks[0][1])
            )
        else:
            if 'a' in message[1].keys():
                asks = message[1]["a"]
                if not asks:
                    asks = [[0, 0]]
                self.resent[symb] = (
                    *self.resent[message[3]][:-1], float(asks[0][0]),
                    float(asks[0][1])
                )
            if 'b' in message[1].keys():
                bids = message[1]["b"]
                if not bids:
                    bids = [[0, 0]]
                self.resent[symb] = (
                    *self.resent[message[3]][:-2], float(bids[0][0]),
                    float(bids[0][1]), self.resent[message[3]][-1]
                )