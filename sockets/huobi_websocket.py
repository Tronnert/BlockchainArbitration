import json
from consts import HUOBI_SUB_FILE, HUOBI_STREAM_NAME, HUOBI_SYMBOLS, \
    HUOBI_MAX_SYMBOLS
from sockets.base_websocket import BaseWebsocket
from requests import get
import gzip


class HuobiWebsocket(BaseWebsocket):
    """Сокет для Huobi"""
    def __init__(self, *args) -> None:
        super().__init__(HUOBI_SUB_FILE, HUOBI_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(HUOBI_MAX_SYMBOLS)
        self.delete_mult_symbols()
        self.add_pattern_to_resent()

    def made_sub_json(self) -> list[dict]:
        """Создание параметров подключения"""
        sub_json = super().made_sub_json()
        ans = []
        for key in self.list_of_symbols.keys():
            sub_json["sub"] = f"market.{key.lower()}.bbo"
            ans.append(sub_json.copy())
        return ans

    @staticmethod
    def get_top_pairs(top: int) -> dict:
        g = sorted(get(HUOBI_SYMBOLS).json()["data"],
                   key=lambda x: float(x["w"]), reverse=True)[:top]
        g = list(map(lambda x: {x["bcdn"] + x["qcdn"]: [x["bcdn"], x["qcdn"]]},
                     filter(lambda x: x["te"], g)))
        symb = {}
        for e in g:
            symb |= e
        return symb

    def process(self, message: dict) -> None:
        """Обработка данных"""
        if "tick" not in message:
            return
        message = message['tick']
        symb = message["symbol"].upper()
        if symb not in self.list_of_symbols:
            return
        cur1, cur2 = self.list_of_symbols[symb]
        self.update_resent(
            symb, base=cur1, quote=cur2, bidFee=self.fee, askFee=self.fee,
            baseWithdrawalFee=self.withdrawal_fee,
            bidPrice=float(message["bid"]), bidQty=float(message["bidSize"]),
            askPrice=float(message["ask"]), askQty=float(message["askSize"]),
            exchange="huobi"
        )

    def on_message(self, ws, mess):
        """Получение данных"""
        self.process(json.loads(gzip.decompress(bytes(mess))))
