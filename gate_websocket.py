from consts import GATES_IO_SUB_FILE, GATES_IO_STREAM_NAME
from base_websocket import BaseWebsocket
import requests
import json
import time
import traceback

class GateWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(GATES_IO_SUB_FILE, GATES_IO_STREAM_NAME, *args)
        self.list_of_symbols = self.get_best_pairs()


    def made_sub_json(self) -> None:
        mess = super().made_sub_json()
        mess["time"] = int(time.time())
        mess["payload"] = list(self.get_best_pairs().keys())
        return json.dumps(mess)

    def get_best_pairs(self):
        ticker_pairs = sorted(requests.get('https://api.gateio.ws/api/v4/spot/tickers').json(), key=lambda x: x['quote_volume'], reverse=True)[:300]
        pairs = {}
        answer = {}
        for i in requests.get('https://api.gateio.ws/api/v4/spot/currency_pairs').json():
            pairs[i['id']] = {'base': i['base'], 'quote': i['quote']}
        for i in ticker_pairs:
            answer[i['currency_pair']] = {'base': pairs[i['currency_pair']]['base'], 'quote': pairs[i['currency_pair']]['quote']}
        return answer


    def on_message(self, ws, mess):
        mess = json.loads(mess)['result']
        if 's' in mess.keys():
            self.resent[mess['s']] = (*self.list_of_symbols[mess['s']].values(), "gate", float(mess["b"]), float(mess["B"]), float(mess["a"]), float(mess["A"]))

