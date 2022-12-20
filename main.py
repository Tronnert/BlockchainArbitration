from scheduler import Scheduler
from sockets.binance_websocket import BinanceWebsocket
from sockets.poloniex_websocket import PoloniexWebsocket
from sockets.kraken_websocket import KrakenWebsocket
from sockets.gate_websocket import GateWebsocket
from sockets.huobi_websocket import HuobiWebsocket
from sockets.bybit_websocket import BybitWebsocket

if __name__ == "__main__":
    filename = "test_bybit.tsv"

    to_start = [BinanceWebsocket(), BybitWebsocket(), PoloniexWebsocket(),
                GateWebsocket(), HuobiWebsocket(), KrakenWebsocket()]
    [socket.start() for socket in to_start]
    scheduler = Scheduler(*to_start, filename=filename)
    scheduler.start()