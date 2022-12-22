from consts import GATES_IO_SUB_FILE, GATES_IO_STREAM_NAME, GATES_IO_SYMBOLS, \
    GATES_IO_MAX_SYMBOLS, GATES_IO_TICKER
from sockets.base_websocket import BaseWebsocket
from requests import get
import time


class GateWebsocket(BaseWebsocket):
    """Сокет для Gate.io"""
    def __init__(self, *args) -> None:
        super().__init__(GATES_IO_SUB_FILE, GATES_IO_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(GATES_IO_MAX_SYMBOLS)
        self.add_pattern_to_resent()

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        message = super().made_sub_json()
        message["time"] = int(time.time())
        message["payload"] = list(self.get_top_pairs(GATES_IO_MAX_SYMBOLS).keys())
        return message

    @staticmethod
    def get_top_pairs(top: int) -> dict:
        ticker_pairs = sorted(
            get(GATES_IO_TICKER).json(),
            key=lambda x: x['quote_volume'], reverse=True
        )[:top]
        pairs = {i["id"]: {"base": i["base"], "quote": i["quote"], "fee": i["fee"]} for i in
                 get(GATES_IO_SYMBOLS).json()}
        answer = {}
        for i in ticker_pairs:
            answer[i['currency_pair']] = (
                pairs[i['currency_pair']]['base'],
                pairs[i['currency_pair']]['quote'],
                float(pairs[i['currency_pair']]['fee']) / 100

            )
        return answer

    def process(self, message: dict) -> None:
        """Обработка данных"""
        message = message["result"]
        if 's' not in message.keys():
            return
        cur1, cur2, fee = self.list_of_symbols[message["s"]]
        self.update_resent(
            message["s"], base=cur1, quote=cur2, exchange="gate",
            bidFee=fee, bidPrice=float(message["b"]),
            bidQty=float(message["B"]), askPrice=float(message["a"]),
            askQty=float(message["A"]), askFee=fee
        )
