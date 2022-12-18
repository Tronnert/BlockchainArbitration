from sheduler import Sheduler
from binance_websocket import BinanceWebsocket
from sockets.poloniex_websocket import PoloniexWebsocket
from kraken_websocket import KrakenWebsocket
from gate_websocket import GateWebsocket
from huobi_websocket import HuobiWebsocket

if __name__ == "__main__":
    # open(GLOBAL_OUTPUT_FILE_NAME, mode="w").write("dt\tbase\tquote\texchange\tbidPrice\tbidQty\taskPrice\taskQty")

    binancewebsocket = BinanceWebsocket()
    binancewebsocket.start()

    poloniexwebsocket = PoloniexWebsocket()
    poloniexwebsocket.start()

    krakenwebsocket = KrakenWebsocket()
    krakenwebsocket.start()

    gatewebsocket = GateWebsocket()
    gatewebsocket.start()

    huobiwebsocket = HuobiWebsocket()
    huobiwebsocket.start()

    sheduler = Sheduler(huobiwebsocket, binancewebsocket, krakenwebsocket, poloniexwebsocket, gatewebsocket)
    sheduler.start()