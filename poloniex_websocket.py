from consts import POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME 
from base_websocket import BaseWebsocket

class PoloniexWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(POLONIEX_SUB_FILE, POLONIEX_STREAM_NAME, *args)

    def on_message(self, ws, mess) -> None:
        mess = super().on_message(ws, mess)
        cur1, cur2 = mess["data"][0]["symbol"].split('_')
        if cur1 in self.different_names.keys():
            cur1 = self.different_names[cur1]
        if cur2 in self.different_names.keys():
            cur2 = self.different_names[cur2]
        self.resent[mess["data"][0]["symbol"]] = (cur1, cur2, "poloniex", float(mess["data"][0]["bids"][0][0]), float(mess["data"][0]["asks"][0][0]))
