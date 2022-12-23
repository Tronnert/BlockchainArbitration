import sys
from requests import get
from consts import COINCAP_API, COINCAP_CRYPTOS, COINCAP_QUOTES, \
    EXCHANGE_RATE_API
from matplotlib import pyplot as plt


def stable_decimal_places(one):
    """Преобразование вещественных чисел"""
    if isinstance(one, float):
        return f'{one:.10f}'
    return str(one)


def printProgressBar(iteration, total):
    """Рисует прикольную шкалу заполнения"""
    length = 50
    percent = str(round((100 * (iteration / float(total))), 1))
    filled_length = int(length * iteration // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\rCollecting data |{bar}| {percent}% Complete')
    sys.stdout.flush()
    if iteration == total:
        print()


def get_mult_symbols() -> set[str]:
    """Возвращает те символы. которым соовтетствуют более одной криптовалюты"""
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINCAP_API,
    }
    resp = get(COINCAP_CRYPTOS, headers=headers).json()["data"]
    symbols = {}
    for i in resp:
        symb = i["symbol"]
        symbols[symb] = symbols.get(symb, 0) + 1
    return set(filter(lambda x: symbols[x] > 1, symbols.keys()))


def get_crypto_quotes(cryptos) -> dict:
    """Для каждой криптовалюты возвращает её последнюю стоимость в долларах"""
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINCAP_API,
    }
    resp = get(COINCAP_QUOTES, headers=headers, params={"symbol": ','.join(cryptos)}).json()["data"]
    ans = {}
    fiat = []
    for key, val in resp.items():
        if val:
            ans[key] = float(val[0]["quote"]['USD']["price"])
        else:
            fiat.append(key)
    if not fiat:
        return ans
    params = {"base": "USD", "symbols": ','.join(fiat)}
    resp2 = get(EXCHANGE_RATE_API, params=params).json()["rates"]
    ans.update({i: 1 / float(resp2[i]) for i in fiat})
    return ans


def draw_bid_exchange(df, exchange: str, size=(10, 10), rows=3, pad=3,
                      exclude_loss=False, prefix='', suffix=''):
    rows_with_exchange = df[df["bidExchange"] == exchange]
    draw_exchanges(rows_with_exchange, size, rows, pad, exclude_loss, prefix, suffix)


def draw_exchanges(df, size=(10, 10), rows=3, pad=3, exclude_loss=False, prefix='', suffix=''):
    """Для каждой биржи, где был ask order, рисует графики распределения прибыли по 10 символам,
    у которых была максимальная прибыль"""
    i = 1
    figure = plt.figure(figsize=size)
    figure.suptitle(f"{prefix}{df['bidExchange'].unique()[0]} - exchnge of bid{suffix}")
    plt.tight_layout(pad=pad)
    len_ = df["askExchange"].nunique()
    for exch1, data in df.groupby("askExchange"):
        sub = figure.add_subplot(rows, (len_ + rows - 1) // rows, i)
        draw_exchange_symbols(sub, data, exclude_loss=exclude_loss)
        i += 1


def draw_exchange_symbols(subplot, df, exclude_loss=False):
    """Рисует графики прибыли символов по времени"""
    symbols = df.groupby("symbol")
    for symb, data in symbols:
        data = data.sort_values("dt")
        if exclude_loss and (data["revenueUSD"] <= 0).all():
            continue
        subplot.plot(data["dt"], data["revenueUSD"], label=symb)
    subplot.axhline(y=0, color='black', linestyle='--', linewidth=3)
    subplot.title.set_text(df["askExchange"].unique()[0])
    subplot.set_xlabel("dt")
    subplot.set_ylabel("revenue in USD")
    subplot.legend()


