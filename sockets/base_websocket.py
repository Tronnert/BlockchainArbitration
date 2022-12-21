import websocket
import threading
import json
from consts import DIFFERENT_NAMES_FILE_NAME, EXCHANGE_FEES
from functions import stable_decimal_places as norm


class BaseWebsocket:
    """Базовый класс сокета"""

    def __init__(self, subfilename: str, streamname: str) -> None:
        self.resent = {}
        self.subfilename = subfilename
        self.streamname = streamname
        self.fee = json.load(open(EXCHANGE_FEES)).get(str(self), None)
        self.different_names = json.load(open(DIFFERENT_NAMES_FILE_NAME))
        self.wsa = websocket.WebSocketApp(
            self.streamname, on_message=self.on_message, on_open=self.on_open
        )
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

    def rename(self, data: list[str]) -> tuple:
        """Переименование пар"""
        return tuple(map(lambda x: self.different_names.get(x, x), data))

    def job(self, now_time: str, file) -> None:
        """Запись данных в файл"""
        x = self.resent.copy().values()
        if not x:
            return
        data = '\n'.join(['\t'.join((now_time, *map(norm, e))) for e in x])
        file.write(data + '\n')

    def on_open(self, ws) -> None:
        """Открытие соединения"""
        print(f"ON {self}")
        data = self.made_sub_json()
        data = [data] if not isinstance(data, list) else data
        [ws.send(sub.replace("'", '"')) for sub in map(json.dumps, data)]

    def on_message(self, ws, mess: str) -> None:
        """Получение данных"""
        self.process(json.loads(mess))

    def process(self, message: dict) -> dict:
        """Обработка данных"""
        return message

    def run_websocket(self) -> None:
        """Запуск сокета"""
        print(f"{self} START")
        self.wsa.run_forever()

    def kill(self):
        """Прекращение работы сокета"""
        self.wsa.keep_running = False
        self.websocket_thread.join()