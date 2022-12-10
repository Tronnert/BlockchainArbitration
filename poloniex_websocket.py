import websocket
from datetime import datetime
import json
from consts import GLOBAL_OUTPUT_FILE_NAME
from binance_websocket import BinanceWebsocket

class PoloniexWebsocket(BinanceWebsocket):
    def job(self) -> None:
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            if 'event' not in self.resent.keys():
                for e in self.resent.values():
                    print("\t".join(map(str, (now_time, *e))), file=file)

    def on_open(self, ws) -> None:
        print("ON OPEN")
        ws.send(open("poloniex.json").read())

    def on_message(self, ws, mess):
        mess = json.loads(mess)
        cur1, cur2 = mess["data"][0]["symbol"].split('_')
        if cur1 in self.different_names.keys():
            cur1 = self.different_names[cur1]
        if cur2 in self.different_names.keys():
            cur2 = self.different_names[cur2]
        self.resent[mess["data"][0]["symbol"]] = (cur1, cur2, "poloniex", float(mess["data"][0]["bids"][0][0]), float(mess["data"][0]["asks"][0][0]))

    def run_websocket(self) -> None:
        print("START")
        wss = f"wss://ws.poloniex.com/ws/public"
        wsa = websocket.WebSocketApp(wss, on_message=self.on_message, on_open=self.on_open)
        wsa.run_forever()