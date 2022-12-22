from consts import POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, POLONIEX_MAX_SYMBOLS, \
    POLONIEX_TICKER, POLONIEX_FEE, API_KEY, API_SECRET
from sockets.base_websocket import BaseWebsocket
import hmac
import hashlib
from requests import get
from time import time
import base64
import json


class PoloniexWebsocket(BaseWebsocket):
    """Сокет для Poloniex"""
    def __init__(self, *args) -> None:
        super().__init__(POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, *args)
        self.fee = self.get_fee()
        self.list_of_symbols = self.get_top_pairs(POLONIEX_MAX_SYMBOLS)
        self.delete_mult_symbols()
        self.add_pattern_to_resent()

    @staticmethod
    def get_withdrawal_fee(currency) -> float:
        link = f'https://api.poloniex.com/currencies/{currency}'
        return float(get(link).json()[currency]['withdrawalFee'])

    @staticmethod
    def get_fee() -> float:
        try:
            # авторизация
            stamp = int(time() * 10 ** 6 // 1000)
            message = f"GET\n/feeinfo\nsignTimestamp={stamp}"
            signature = base64.b64encode(
                hmac.new(bytes(API_SECRET, 'utf-8'), msg=bytes(message, 'utf-8'),
                         digestmod=hashlib.sha256).digest()).decode()
            header = {"signatureMethod": "HmacSHA256", "signatureVersion": "2",
                      "signTimestamp": str(stamp),
                      "key": API_KEY,
                      "signature": signature}
            resp = get(POLONIEX_FEE, headers=header)
            if resp.status_code != 200:
                raise TypeError
            return float(resp.json()["takerRate"])
        except TypeError:
            return 1

    def get_top_pairs(self, top: int) -> dict:
        ticker24 = sorted(get(POLONIEX_TICKER).json(),
                          key=lambda x: x["tradeCount"], reverse=True)[:top]
        pairs = {i["symbol"]: self.rename(i["symbol"].split('_')) for i in ticker24}
        return pairs

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        sub_json["symbols"] = list(self.list_of_symbols.keys())
        # sub_json["symbols"] = list(map(lambda x: "_".join(x),
        #                                self.list_of_symbols.values()))
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        if "data" not in message:
            return
        message = message["data"][0]
        if message["symbol"] not in self.list_of_symbols:
            return
        cur1, cur2 = message["symbol"].split('_')
        #withdrawal_fee = self.get_withdrawal_fee(cur1)
        ask = [0, 0] if not message["asks"] else [float(message["asks"][0][0]), float(message["asks"][0][1])]
        bid = [0, 0] if not message["bids"] else [float(message["bids"][0][0]), float(message["bids"][0][1])]
        self.update_resent(
            message["symbol"], base=cur1, quote=cur2, exchange="poloniex",
            baseWithdrawalFee=self.withdrawal_fee,
            bidPrice=bid[0], bidQty=bid[1], bidFee=self.fee,
            askPrice=ask[0], askQty=ask[1], askFee=self.fee


        )