GLOBAL_OUTPUT_FILE_NAME = "logs3.tsv"
GLOBAL_OUTPUT_FOLDER = "logs/"

DIFFERENT_NAMES_FILE_NAME = "json/different_names.json"

BINANCE_SUB_FILE = "json/binance_sub.json"
BINANCE_STREAM_NAME = "wss://stream.binance.com:9443/ws/tronnert_stream"
BINANCE_FEES = "https://eapi.binance.com/eapi/v1/exchangeInfo"

POLONIEX_SUB_FILE = "json/poloniex_sub.json"
POLONIEX_STREAM_NAME = "wss://ws.poloniex.com/ws/public"

KRAKEN_SUB_FILE = "json/kraken_sub.json"
KRAKEN_STREAM_NAME = "wss://ws.kraken.com"

GATES_IO_STREAM_NAME = "wss://api.gateio.ws/ws/v4/"
GATES_IO_SUB_FILE = "json/gate_io_sub.json"

HUOBI_SUB_FILE = "json/huobi_sub.json"
HUOBI_STREAM_NAME = "wss://api.huobi.pro/ws"

BINANCE_SYMBOLS = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/24hr"
BINANCE_MAX_SYMBOLS = 300
BINANCE_STEP = 50

POLONIEX_TICKER = "https://api.poloniex.com/markets/ticker24h"
POLONIEX_MAX_SYMBOLS = 300

KRAKEN_SYMBOLS = "https://api.kraken.com/0/public/AssetPairs"

GATES_IO_SYMBOLS = "https://api.gateio.ws/api/v4/spot/currency_pairs"
GATES_IO_TICKER = "https://api.gateio.ws/api/v4/spot/tickers"
GATES_IO_MAX_SYMBOLS = 300

HUOBI_SYMBOLS = "https://api.huobi.pro/v2/settings/common/symbols"
HUOBI_MAX_SYMBOLS = 300

BYBIT_STREAM_NAME = "wss://stream.bybit.com/contract/usdt/public/v3"
BYBIT_SUB_FILE = "json/bybit_sub.json"
BYBIT_SYMBOLS = "https://api.bybit.com/derivatives/v3/public/instruments-info"
BYBIT_TICKER = "https://api.bybit.com/derivatives/v3/public/tickers"


BITGET_STREAM_NAME = "wss://ws.bitget.com/spot/v1/stream"
BITGET_SUB_FILE = "json/bitget_sub.json"
BITGET_SYMBOLS = "https://api.bitget.com/api/spot/v1/public/products"
BITGET_TICKER = "https://api.bitget.com/api/spot/v1/market/tickers"
BITGET_MAX_SYMBOLS = 300

EXCHANGE_FEES = "json/exchange_fee.json"
# from requests import get
# from pprint import pprint
#
# resp = get("https://eapi.binance.com/eapi/v1/exchangeInfo").json()["optionSymbols"]
# print({i["underlying"]: float(i["takerFeeRate"]) for i in resp})