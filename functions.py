import sys
from requests import get
from consts import COINCAP_API, COINCAP_CRYPTOS, COINCAP_QUOTES, \
    EXCHANGE_RATE_API


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

