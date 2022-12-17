import websocket
from datetime import datetime
import threading
import json
from consts import GLOBAL_OUTPUT_FILE_NAME, DIFFERENT_NAMES_FILE_NAME


class BaseWebsocket():
    def __init__(self, subfilename, streamname) -> None:
        self.resent = dict()
        self.subfilename = subfilename
        self.streamname = streamname
        self.different_names = json.load(open(DIFFERENT_NAMES_FILE_NAME))
        self.websocket_thread = threading.Thread(target=self.run_websocket)
        self.list_of_symbols = {}

    def return_self_name(self):
        return self.__class__.__name__

    def start(self) -> None:
        self.websocket_thread.start()

    def made_sub_json(self) -> None:
        return json.load(open(self.subfilename))

    def job(self) -> None:
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            x = self.resent.copy().values()
            for e in x:
                print("\t".join(map(str, (now_time, *map(stable_decimal_places, e)))), file=file)

    def on_open(self, ws) -> None:
        print(f"ON {self.return_self_name()}")
        sub_json = self.made_sub_json()
        if not isinstance(sub_json, list):
            sub_json = [sub_json]
        for single_sub in sub_json:
            ws.send(str(single_sub).replace("'", '''"'''))

    def on_message(self, ws, mess):
        mess = json.loads(mess)
        return mess

    def run_websocket(self) -> None:
        print(f"{self.return_self_name()} START")
        wsa = websocket.WebSocketApp(self.streamname, on_message=self.on_message, on_open=self.on_open)
        wsa.run_forever()

def stable_decimal_places(one):
    if isinstance(one, float):
        return f'{one:.10f}'
    return one