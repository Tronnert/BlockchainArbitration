import websocket
from datetime import datetime
import time
import threading
import schedule
import json
from consts import GLOBAL_OUTPUT_FILE_NAME

class BinanceWebsocket():
    def __init__(self) -> None:
        self.resent = dict()
        self.schedule_thread = threading.Thread(target=self.scheduling)
        self.websocket_thread = threading.Thread(target=self.run_websocket)

    def start(self) -> None:
        self.schedule_thread.start()
        self.websocket_thread.start()

    def job(self) -> None:
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            for e in self.resent.values():
                print("\t".join(map(str, (now_time, *e))), file=file)

    def scheduling(self) -> None:
        schedule.every(1).seconds.do(self.job)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def on_open(self, ws) -> None:
        print("ON OPEN")
        ws.send(open("binance_sub.json").read())

    def on_message(self, ws, mess):
        mess = json.loads(mess)
        self.resent[mess["s"]] = (mess["s"], "binance", float(mess["b"]), float(mess["a"]))

    def run_websocket(self) -> None:
        print("START")
        stream = "tronnert_stream"
        wss = f"wss://stream.binance.com:9443/ws/{stream}"
        wsa = websocket.WebSocketApp(wss, on_message=self.on_message, on_open=self.on_open)
        wsa.run_forever()
