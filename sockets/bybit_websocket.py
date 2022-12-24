from consts import BYBIT_SYMBOLS, BYBIT_STREAM_NAME, BYBIT_SUB_FILE, BYBIT_TICKER
from sockets.base_websocket import BaseWebsocket
from requests import get


class BybitWebsocket(BaseWebsocket):
    """Сокет для Bybit"""
    def __init__(self, *args) -> None:
        super().__init__(BYBIT_SUB_FILE, BYBIT_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs()
        self.delete_mult_symbols()
        self.add_pattern_to_resent()

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
        sub_json["args"] = list(map(lambda x: "orderbook.1." + str(x),
                                    self.list_of_symbols.keys()))
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        if "data" not in message:
            return
        message = message["data"]
        if message["s"] not in self.list_of_symbols:
            return
        cur1, cur2 = self.list_of_symbols[message["s"]]
        # пришел снапшот, нужно загрузить данные
        if message["b"] and message["a"]:
            bids, asks = message["b"], message["a"]
            self.update_resent(
                message["s"], base=cur1, quote=cur2, exchange="bybit",
                baseWithdrawalFee=self.withdrawal_fee,
                bidFee=self.fee, bidPrice=float(bids[0][0]),
                bidQty=float(bids[0][1]), askPrice=float(asks[0][0]),
                askQty=float(asks[0][1]), askFee=self.fee
            )
        else:  # данные надо обновить
            if message['b']:
                bids = message["b"]
                self.update_resent(
                    message["s"], bidPrice=float(bids[0][0]), bidQty=float(bids[0][1])
                )
            if message["a"]:
                asks = message["a"]
                self.update_resent(
                    message["s"], askPrice=float(asks[0][0]), askQty=float(asks[0][1])
                )
