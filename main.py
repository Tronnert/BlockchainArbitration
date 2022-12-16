from get_top100pairs import get_top100pars
from sheduler import Sheduler
from binance_websocket import BinanceWebsocket
from poloniex_websocket import PoloniexWebsocket
from kraken_websocket import KrakenWebsocket
from gate_websocket import GateWebsocket


if __name__ == "__main__":
    top100pairs = get_top100pars()

    binancewebsocket = BinanceWebsocket(top100pairs)
    binancewebsocket.start()

    poloniexwebsocket = PoloniexWebsocket(top100pairs)
    poloniexwebsocket.start()

    krakenwebsocket = KrakenWebsocket(top100pairs)
    krakenwebsocket.start()

    gatewebsocket = GateWebsocket(top100pairs)
    gatewebsocket.start()

    sheduler = Sheduler(binancewebsocket, poloniexwebsocket, krakenwebsocket, gatewebsocket) #, poloniexWebsocket, krakenWebsocket)
    sheduler.start()