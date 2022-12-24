from scheduler import Scheduler
from sockets.binance_websocket import BinanceWebsocket
from sockets.poloniex_websocket import PoloniexWebsocket
from sockets.kraken_websocket import KrakenWebsocket
from sockets.gate_websocket import GateWebsocket
from sockets.huobi_websocket import HuobiWebsocket
from sockets.bybit_websocket import BybitWebsocket
from sockets.bitget_websocket import BitgetWebsocket
from argparse import ArgumentParser, BooleanOptionalAction
import threading
from datetime import datetime


all_exchanges = {"binance", "bybit", "bitget", "poloniex", "gate", "huobi", "kraken"}
parser = ArgumentParser("Получение исторических данных за промежуток времени")
parser.add_argument("--duration", type=int, default=60, nargs='?')
parser.add_argument("--filename", nargs="?")
parser.add_argument('--progress_bar', action=BooleanOptionalAction)
parser.add_argument('--include', default=all_exchanges, nargs="+")
parser.add_argument("--exclude", default=set(), nargs="+")


if __name__ == '__main__':
    args = parser.parse_args()
    progress = False if args.progress_bar is None else True
    filename = datetime.utcnow().strftime(f"%Y.%m.%d_%H.%M.%S_{args.duration}.tsv") if args.filename is None else args.filename
    event = threading.Event()
    exchanges = all_exchanges.intersection(set(args.include)) - set(args.exclude)
    to_start = [eval(f"{i.capitalize()}Websocket()") for i in exchanges]
    [socket.start() for socket in to_start]
    scheduler = Scheduler(*to_start, duration=args.duration, event=event,
                          filename=args.filename, progress=progress)
    scheduler.start()
    scheduler.schedule_thread.join(args.duration)
    event.set()
    scheduler.schedule_thread.join()