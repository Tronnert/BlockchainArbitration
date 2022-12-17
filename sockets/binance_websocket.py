from consts import BINANCE_SUB_FILE, BINANCE_STREAM_NAME, BINANCE_MAX_SYMBOLS, \
    BINANCE_STEP as step, BINANCE_SYMBOLS, BINANCE_TICKER
from sockets.base_websocket import BaseWebsocket
from requests import get


class BinanceWebsocket(BaseWebsocket):
    """Сокет для Binance"""
    def __init__(self, *args) -> None:
        super().__init__(BINANCE_SUB_FILE, BINANCE_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(BINANCE_MAX_SYMBOLS)

    def made_sub_json(self) -> list[dict]:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        ans = []
        for e in range(0, len(self.list_of_symbols), step):
            sub_json["params"] = list(map(
                lambda x: x.lower() + "@bookTicker",
                list(self.list_of_symbols.keys())[e: e + step])
            )
            ans.append(sub_json.copy())
        return ans

    @staticmethod
    def get_top_pairs(top: int) -> dict:
        symbols_list = get(BINANCE_SYMBOLS).json()["symbols"]
        symbols = {e["symbol"]: e for e in symbols_list}

        ticker24 = get(BINANCE_TICKER).json()
        ticker24 = list(sorted(ticker24, key=lambda x: x["count"], reverse=True)[:top])
        ticker24 = list(map(lambda x: x | symbols[x["symbol"]], ticker24))

        return {pair["symbol"]: (pair["baseAsset"], pair["quoteAsset"]) for pair in ticker24}

    def process(self, message: dict) -> None:
        """Обработка данных"""
        self.resent[message["s"]] = (
            *self.list_of_symbols[message["s"]], "binance", float(message["b"]),
            float(message["B"]), float(message["a"]), float(message["A"])
        )
