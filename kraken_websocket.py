from consts import KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME
from base_websocket import BaseWebsocket

class KrakenWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(KRAKEN_SUB_FILE, KRAKEN_STREAM_NAME, *args)

    def made_sub_json(self) -> None:
        sub_json = super().made_sub_json()
        sub_json["pair"] = list(map(lambda x: "/".join(x), self.list_of_symbols.values()))
        print(sub_json)
        return sub_json

    def on_message(self, ws, mess):
        mess = super().on_message(ws, mess)
        cur1, cur2 = map(lambda x: self.different_names.get(x, x), mess[3].split('/'))
        # if cur1 in self.different_names.keys():
        #     cur1 = self.different_names[cur1]
        # if cur2 in self.different_names.keys():
        #     cur2 = self.different_names[cur2]
        if 'as' in mess[1].keys():
            self.resent[mess[3]] = (cur1, cur2, "kraken", float(mess[1]["bs"][0][0]), float(mess[1]["as"][0][0]))
        elif 'a' in mess[1].keys():
            self.resent[mess[3]] = (*self.resent[mess[3]][:-1], float(mess[1]["a"][0][0]))
        elif 'b' in mess[1].keys():
            self.resent[mess[3]] = (*self.resent[mess[3]][:-2], float(mess[1]["b"][0][0]), self.resent[mess[3]][-1])