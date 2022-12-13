from consts import BINANCE_SUB_FILE, BINANCE_STREAM_NAME
from base_websocket import BaseWebsocket

class BinanceWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(BINANCE_SUB_FILE, BINANCE_STREAM_NAME, *args)

    def on_message(self, ws, mess):
        mess = super().on_message(ws, mess)
        self.resent[mess["s"]] = (*self.list_of_symbols[mess["s"]], "binance", float(mess["b"]), float(mess["a"]))
