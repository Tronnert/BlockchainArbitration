import websocket
from datetime import datetime
import json
from consts import GLOBAL_OUTPUT_FILE_NAME
from binance_websocket import BinanceWebsocket

class KrakenWebsocket(BinanceWebsocket):
    def job(self) -> None:
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:

            if 'event' not in self.resent.keys():
                for e in self.resent.values():
                    print("\t".join(map(str, (now_time, *e))), file=file)

    def on_open(self, ws) -> None:
        print("ON OPEN")
        ws.send(open("kraken.json").read())

    def on_message(self, ws, mess):
        mess = json.loads(mess)
        cur1, cur2 = mess[3].split('/')
        if cur1 in self.different_names.keys():
            cur1 = self.different_names[cur1]
        if cur2 in self.different_names.keys():
            cur2 = self.different_names[cur2]
        if 'as' in mess[1].keys():
            self.resent[mess[3]] = (cur1, cur2, "kraken", float(mess[1]["bs"][0][0]), float(mess[1]["as"][0][0]))
        elif 'a' in mess[1].keys():
            self.resent[mess[3]] = (*self.resent[mess[3]][:-1], float(mess[1]["a"][0][0]))
        elif 'b' in mess[1].keys():
            self.resent[mess[3]] = (*self.resent[mess[3]][:-2], float(mess[1]["b"][0][0]), self.resent[mess[3]][-1])


    def run_websocket(self) -> None:
        print("START")
        wss = f"wss://ws.kraken.com"
        wsa = websocket.WebSocketApp(wss, on_message=self.on_message, on_open=self.on_open)
        wsa.run_forever()