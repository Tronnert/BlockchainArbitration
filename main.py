from get_top100pairs import get_top100pars
from sheduler import Sheduler
from binance_websocket import BinanceWebsocket
from poloniex_websocket import PoloniexWebsocket


if __name__ == "__main__":
    top100pairs = get_top100pars()
    binancewebsocket = BinanceWebsocket()
    binancewebsocket.start()
    poloniexWebsocket = PoloniexWebsocket()
    poloniexWebsocket.start()
    sheduler = Sheduler(binancewebsocket, poloniexWebsocket)
    sheduler.start()
    
