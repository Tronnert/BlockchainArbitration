from binance_websocket import BinanceWebsocket
from poloniex_websocket import PoloniexWebsocket


if __name__ == "__main__":
    binancewebsocket = BinanceWebsocket()
    poloniexWebsocket = PoloniexWebsocket()
    poloniexWebsocket.start()
    binancewebsocket.start()