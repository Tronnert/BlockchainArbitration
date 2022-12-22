from consts import BITGET_STREAM_NAME, BITGET_SUB_FILE, BITGET_SYMBOLS, \
    BITGET_TICKER, BITGET_MAX_SYMBOLS
from sockets.base_websocket import BaseWebsocket
from requests import get


class BitgetWebsocket(BaseWebsocket):
    """Сокет для Bitget"""
    def __init__(self, *args) -> None:
        super().__init__(BITGET_SUB_FILE, BITGET_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(BITGET_MAX_SYMBOLS)
        self.add_pattern_to_resent()

    @staticmethod
    def get_top_pairs(top: int) -> dict:
        ticker = sorted(get(BITGET_TICKER).json()["data"],
                        key=lambda x: float(x["usdtVol"]), reverse=True)[:top]
        names = {u["symbol"] for u in ticker}
        symbols = {i["symbolName"]: (i["baseCoin"], i["quoteCoin"], float(i["takerFeeRate"]))
                   for i in get(BITGET_SYMBOLS).json()["data"]}
        return {i: j for i, j in symbols.items() if i in names}

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        sub_json = super().made_sub_json()
        for symb in self.list_of_symbols:
            sub_json["args"].append({"instType": "SP", "channel": "books", "instId": symb})
        return sub_json

    def process(self, message: dict) -> None:
        """Обработка данных"""
        if "event" in message:  # сообщение о подписке
            return
        symb = message["arg"]["instId"]
        cur1, cur2, fee = self.list_of_symbols[symb]
        asks = map(lambda x: (float(x[0]), float(x[1])), message["data"][0]["asks"])
        bids = map(lambda x: (float(x[0]), float(x[1])), message["data"][0]["bids"])
        best_bid = self.get_first_not_null(asks)
        best_ask = self.get_first_not_null(bids)
        if message["action"] == "snapshot":  # новые данные
            self.update_resent(
                symb, base=cur1, quote=cur2, exchange="bitget", takerFee=fee,
                bidPrice=best_bid[0], bidQty=best_bid[1], askPrice=best_ask[0],
                askQty=best_ask[1]
            )
        else:  # обновление данных
            # если последнего ордера на покупку или продажу нет, надо взять
            # следующий ордер, иначе взять минимльный/максимальный из текущего и нового)
            ask = self.resent[symb]["askPrice"], self.resent[symb]["askQty"]
            ask = best_ask if self.get_by_price(ask, asks) == 0 else min(ask, best_ask, key=lambda x: x[0])
            bid = self.resent[symb]["bidPrice"], self.resent[symb]["bidQty"]
            bid = best_bid if self.get_by_price(bid, bids) == 0 else max(bid, best_bid, key=lambda x: x[0])
            self.update_resent(
                symb, base=cur1, quote=cur2, exchange="bitget", takerFee=fee,
                bidPrice=bid[0], bidQty=bid[1], askPrice=ask[0], askQty=ask[1]
           )

    @staticmethod
    def get_first_not_null(data) -> tuple[float, float]:
        """Возвращает первую пару, где количество не равно 0"""
        data = map(lambda x: (float(x[0]), float(x[1])), data)
        for el in data:
            if el[1] != 0:
                return el
        return 0.0, 0.0

    @staticmethod
    def get_by_price(price, data) -> float:
        """Возвращает количество по заданнйо цене"""
        for el in data:
            if el[0] == price:
                return el[1]
