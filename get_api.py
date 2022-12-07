from requests import get
import json


def only_hot(one):
    return one["count"] >=10000


# print(json.dumps(get("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": "ETHBTC"}).json(), indent=4))
with open("24.json", mode="w") as file:
    answer = list(filter(only_hot, get("https://api.binance.com/api/v3/ticker/24hr").json()))
    print(len(answer))
    file.write(json.dumps(answer, indent=4))