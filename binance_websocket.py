import asyncio
import websocket

def run():
    print("start")
    stream = "btcusdt@depth5"
    wss = f"wss://stream.binance.com:9443/ws/{stream}"
    wsa = websocket.WebSocketApp(wss, on_message=lambda x, y: print(y))
    wsa.run_forever()


run()