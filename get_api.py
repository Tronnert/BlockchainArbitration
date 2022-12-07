from requests import get


print(get("https://api.coingecko.com/api/v3/ping").json())