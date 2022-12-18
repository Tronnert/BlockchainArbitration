from scheduler import Scheduler
from sockets.binance_websocket import BinanceWebsocket
from sockets.poloniex_websocket import PoloniexWebsocket
from sockets.kraken_websocket import KrakenWebsocket
from sockets.gate_websocket import GateWebsocket
from sockets.huobi_websocket import HuobiWebsocket

if __name__ == "__main__":
    # open(GLOBAL_OUTPUT_FILE_NAME, mode="w").write("dt\tbase\tquote\texchange\tbidPrice\tbidQty\taskPrice\taskQty")
    to_start = [KrakenWebsocket()]
    [socket.start() for socket in to_start]
    scheduler = Scheduler(*to_start)
    scheduler.start()