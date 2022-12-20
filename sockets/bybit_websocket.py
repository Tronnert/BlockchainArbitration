from consts import BYBIT_SYMBOLS, BYBIT_STREAM_NAME, BYBIT_SUB_FILE, BYBIT_TICKER
from sockets.base_websocket import BaseWebsocket
from requests import get


class BybitWebsocket(BaseWebsocket):
    """Сокет для Bybit"""
    def __init__(self, *args) -> None:
        super().__init__(BYBIT_SUB_FILE, BYBIT_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs()

    @staticmethod
    def get_top_pairs():
        resp1 = get(BYBIT_SYMBOLS).json()["result"]["list"]
        resp1 = list(
            filter(lambda x: "USD" not in (x["baseCoin"], x["quoteCoin"]),
                   resp1))
        ticker = [i["symbol"] for i in get(BYBIT_TICKER).json()["result"]["list"]]
        return {i["symbol"]: (i["baseCoin"], i["quoteCoin"])
                for i in resp1 if i["symbol"] in ticker}

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        sub_json["args"] = list(map(lambda x: "orderbook.1." + "".join(x),
                                    self.list_of_symbols.values()))
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        message = message["data"]
        cur1, cur2 = self.list_of_symbols[message["s"]]
        # пришел снапшот, нужно загрузить данные
        if message["b"] and message["a"]:
            self.resent[message["s"]] = (
                cur1, cur2, "bybit", float(message["b"][0][0]),
                float(message["b"][0][1]), float(message["a"][0][0]),
                float(message["a"][0][1])
            )
        else:  # данные надо обновить
            if message['b']:
                last_ask = self.resent[message["s"]][-2:]
                self.resent[message["s"]] = (
                    cur1, cur2, "bybit", float(message["b"][0][0]),
                    float(message["b"][0][1]), *last_ask
                )
            if message["a"]:
                last_bid = self.resent[message["a"]][3:5]
                self.resent[message["s"]] = (
                    cur1, cur2, "bybit", *last_bid, float(message["a"][0][0]),
                    float(message["a"][0][1])
                )
