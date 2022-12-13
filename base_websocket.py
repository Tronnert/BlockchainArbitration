import websocket
from datetime import datetime
import threading
import json
from consts import GLOBAL_OUTPUT_FILE_NAME, DIFFERENT_NAMES_FILE_NAME

class BaseWebsocket():
    def __init__(self, subfilename, streamname, list_of_symbols) -> None:
        self.resent = dict()
        self.subfilename = subfilename
        self.streamname = streamname
        self.list_of_symbols = list_of_symbols
        self.different_names = json.load(open(DIFFERENT_NAMES_FILE_NAME))
        self.websocket_thread = threading.Thread(target=self.run_websocket)

    def return_self_name(self):
        return self.__class__.__name__

    def start(self) -> None:
        self.websocket_thread.start()

    def made_sub_json(self) -> None:
        return json.load(open(self.subfilename))

    def job(self) -> None:
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            for e in self.resent.values():
                print("\t".join(map(str, (now_time, *e))), file=file)

    def on_open(self, ws) -> None:
        print(f"ON {self.return_self_name()}")
        ws.send(str(self.made_sub_json()).replace("'", '''"'''))

    def on_message(self, ws, mess):
        mess = json.loads(mess)
        return mess

    def run_websocket(self) -> None:
        print(f"{self.return_self_name()} START")
        wsa = websocket.WebSocketApp(self.streamname, on_message=self.on_message, on_open=self.on_open)
        wsa.run_forever()
