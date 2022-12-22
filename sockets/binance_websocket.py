from consts import BINANCE_SUB_FILE, BINANCE_STREAM_NAME, BINANCE_MAX_SYMBOLS, \
    BINANCE_STEP as step, BINANCE_SYMBOLS, BINANCE_TICKER, BINANCE_FEES
from sockets.base_websocket import BaseWebsocket
from requests import get


class BinanceWebsocket(BaseWebsocket):
    """Сокет для Binance"""
    def __init__(self, *args) -> None:
        super().__init__(BINANCE_SUB_FILE, BINANCE_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(BINANCE_MAX_SYMBOLS)
        self.add_pattern_to_resent()

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
    def get_fees():
        """Получение комиссий у символов"""
        resp = get(BINANCE_FEES).json()["optionSymbols"]
        return {i["underlying"]: float(i["takerFeeRate"]) for i in resp}

    def get_top_pairs(self, top: int) -> dict:
        symbols_list = get(BINANCE_SYMBOLS).json()["symbols"]
        symbols = {e["symbol"]: e for e in symbols_list}

        ticker24 = get(BINANCE_TICKER).json()
        ticker24 = list(sorted(ticker24, key=lambda x: x["count"], reverse=True)[:top])
        ticker24 = list(map(lambda x: x | symbols[x["symbol"]], ticker24))
        fees = self.get_fees()
        return {pair["symbol"]: (
            pair["baseAsset"], pair["quoteAsset"], fees.get(pair["symbol"], self.fee)
        ) for pair in ticker24}

    def process(self, message: dict) -> None:
        """Обработка данных"""
        if "s" not in message:
            return
        symb = message["s"]
        cur1, cur2, fee = self.list_of_symbols[symb]
        self.update_resent(
            symb, base=cur1, quote=cur2, exchange="binance",
            bidPrice=float(message["b"]), bidQty=float(message["B"]),
            askPrice=float(message["a"]), askQty=float(message["A"]),
            bidFee=fee, askFee=fee
        )
