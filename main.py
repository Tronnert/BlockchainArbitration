from sheduler import Sheduler
from binance_websocket import BinanceWebsocket
from poloniex_websocket import PoloniexWebsocket
from kraken_websocket import KrakenWebsocket
from gate_websocket import GateWebsocket
from huobi_websocket import HuobiWebsocket


if __name__ == "__main__":

    binancewebsocket = BinanceWebsocket()
    binancewebsocket.start()

    poloniexwebsocket = PoloniexWebsocket()
    poloniexwebsocket.start()

    krakenwebsocket = KrakenWebsocket()
    krakenwebsocket.start()

    gatewebsocket = GateWebsocket(top100pairs)
    gatewebsocket.start()

    huobiwebsocket = HuobiWebsocket()
    huobiwebsocket.start()

    sheduler = Sheduler(huobiwebsocket, binancewebsocket, krakenwebsocket, poloniexwebsocket, gatewebsocket)
    sheduler.start()