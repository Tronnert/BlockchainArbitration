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


def draw_symb_or_exchange(df, name: str, type_name="exchange", size=(10, 10), rows=3, pad=3,
                      exclude_loss=False, prefix='', suffix=''):
    """Если type_name='exchange', строит для каждой пары (name, askExchange) 
    распределение прибыли по топ-5 символам по средней прибыли 
    (name здесь - биржа с ордером продажи).
    Если type_name='symbol', строит для символа распределение его прибыли по 
    всем парам (bidExchange, askExchange)"""
    if type_name == "exchange":
        data_rows = df[df["bidExchange"] == name]
    else:
        data_rows = df[df["symbol"] == name]
    draw_sub(data_rows, name, type_name=type_name, size=size, rows=rows, 
             pad=pad,  exclude_loss=exclude_loss, prefix=prefix, suffix=suffix)

    
def draw_sub(df, name, type_name="exchange", size=(10, 10), rows=3, pad=3, 
             exclude_loss=False, prefix='', suffix=''):
    i = 1
    figure = plt.figure(figsize=size)
    figure.suptitle(f"{prefix}{name}{suffix}")
    plt.tight_layout(pad=pad)
    group = df.groupby("askExchange") if type_name == "exchange" else df.groupby(["bidExchange", "askExchange"])
    len_ = len(group)
    for name, data in group:
        sub = figure.add_subplot(rows, (len_ + rows - 1) // rows, i)
        draw_based_on_dt(sub, data, name, type_name=type_name, exclude_loss=exclude_loss)
        i += 1


def draw_based_on_dt(subplot, df, title, type_name="exchange", exclude_loss=False):
    title = '/'.join(title) if isinstance(title, tuple) else title
    """Рисует графики по времени"""
    if type_name == "exchange":
        for name, data in df.groupby("symbol"):
            data = data.sort_values("dt")
            if exclude_loss and (data["revenueUSD"] <= 0).all():
                continue
            subplot.plot(data["dt"], data["revenueUSD"], label=name)
    else:
        df = df.sort_values("dt")
        if not exclude_loss or exclude_loss and not (df["revenueUSD"] <= 0).all():
            subplot.plot(df["dt"], df["revenueUSD"], label=title)
    subplot.axhline(y=0, color='black', linestyle='--', linewidth=3)
    subplot.title.set_text(title)
    subplot.set_xlabel("dt")
    subplot.set_ylabel("revenue in USD")
    subplot.legend()


