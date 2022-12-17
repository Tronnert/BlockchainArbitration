import websocket
from datetime import datetime
import threading
import json
from consts import GLOBAL_OUTPUT_FILE_NAME, DIFFERENT_NAMES_FILE_NAME


class BaseWebsocket:
    """Базовый класс сокета"""

    def __init__(self, subfilename: str, streamname: str) -> None:
        self.resent = {}
        self.subfilename = subfilename
        self.streamname = streamname
        self.different_names = json.load(open(DIFFERENT_NAMES_FILE_NAME))
        self.websocket_thread = threading.Thread(target=self.run_websocket)
        self.list_of_symbols = {}

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return repr(self)

    def start(self) -> None:
        self.websocket_thread.start()

    def made_sub_json(self) -> dict:
        """Создание параметров соединения"""
        return json.load(open(self.subfilename))

    def job(self) -> None:
        """Запись данных в файл"""
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            x = self.resent.copy().values()
            [print('\t'.join(map(str, (str(now_time), *e))), file=file) for e in x]

    def on_open(self, ws) -> None:
        """Открытие соединения"""
        print(f"ON {self}")
        data = self.made_sub_json()
        data = [data] if not isinstance(data, list) else data
        [ws.send(sub) for sub in map(json.dumps, data)]

    def on_message(self, ws, mess: str) -> None:
        """Получение данных"""
        self.process(json.loads(mess))

    def process(self, message: dict) -> None:
        """Обработка данных"""
        return message

    def run_websocket(self) -> None:
        """Запуск сокета"""
        print(f"{self} START")
        wsa = websocket.WebSocketApp(
            self.streamname, on_message=self.on_message, on_open=self.on_open
        )
        wsa.run_forever()


def stable_decimal_places(one):
    if isinstance(one, float):
        return f'{one:.10f}'
    return str(one)