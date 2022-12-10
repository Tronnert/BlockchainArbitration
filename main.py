from get_top100pairs import get_top100pars
from sheduler import Sheduler
from binance_websocket import BinanceWebsocket


if __name__ == "__main__":
    top100pairs = get_top100pars()
    binancewebsocket = BinanceWebsocket()
    binancewebsocket.start()
    sheduler = Sheduler(binancewebsocket)
    sheduler.start()
    
