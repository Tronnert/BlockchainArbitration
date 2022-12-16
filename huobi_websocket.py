from consts import HUOBI_SUB_FILE, HUOBI_STREAM_NAME
from base_websocket import BaseWebsocket
from requests import get
import gzip

class HuobiWebsocket(BaseWebsocket):
    def __init__(self, *args) -> None:
        super().__init__(HUOBI_SUB_FILE, HUOBI_STREAM_NAME, *args)
        self.list_of_symbols = self.get_top_pairs(300)

    def made_sub_json(self) -> None:
        sub_json = super().made_sub_json()
        ans = []
        for key in self.list_of_symbols.keys():
            sub_json["sub"] = f"market.{key.lower()}.bbo"
            ans.append(sub_json.copy())
        return ans

    def get_top_pairs(self, top):
        g = get("https://api.huobi.pro/v2/settings/common/symbols").json()["data"]
        g.sort(key=lambda x: float(x["w"]), reverse=True)
        g = g[:top]
        g = list(map(lambda x: {x["bcdn"] + x["qcdn"]: [x["bcdn"], x["qcdn"]]}, filter(lambda x: x["te"], g)))
        symb = {}
        for e in g:
            symb |= e
        return symb

    def on_message(self, ws, mess):
        mess = super().on_message(ws, gzip.decompress(mess))
        if "tick" in mess:
            mess = mess["tick"]
            self.resent[mess["symbol"]] = (*self.list_of_symbols[mess["symbol"].upper()], "huobi", float(mess["bid"]), float(mess["ask"]))
