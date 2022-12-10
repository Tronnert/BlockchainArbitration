from binance_websocket import BinanceWebsocket
from poloniex_websocket import PoloniexWebsocket
from kraken_websocket import KrakenWebsocket


if __name__ == "__main__":
    binancewebsocket = BinanceWebsocket()
    binancewebsocket.start()
    poloniexWebsocket = PoloniexWebsocket()
    poloniexWebsocket.start()
    kraken_websocket = KrakenWebsocket()
    kraken_websocket.start()
