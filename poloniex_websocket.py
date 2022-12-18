from consts import POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, POLONIEX_MAX_SYMBOLS, \
    POLONIEX_TICKER
from sockets.base_websocket import BaseWebsocket
from requests import get


class PoloniexWebsocket(BaseWebsocket):
    """Сокет для Poloniex"""
    def __init__(self, *args) -> None:
        super().__init__(POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(POLONIEX_MAX_SYMBOLS)

    def get_top_pairs(self, top: int) -> dict:
        ticker24 = sorted(get(POLONIEX_TICKER).json(),
                          key=lambda x: x["tradeCount"], reverse=True)[:top]
        pairs = {i["symbol"].replace('_', ''): self.rename(i["symbol"].split('_')) for i in ticker24}
        return pairs

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        sub_json["symbols"] = list(map(lambda x: "_".join(x),
                                       self.list_of_symbols.values()))
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        message = message["data"][0]
        cur1, cur2 = message["symbol"].split('_')
        self.resent[message["symbol"]] = (
            cur1, cur2, "poloniex", float(message["bids"][0][0]),
            float(message["bids"][0][1]), float(message["asks"][0][0]),
            float(message["asks"][0][1])
        )
