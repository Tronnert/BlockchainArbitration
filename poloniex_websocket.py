import websocket
from datetime import datetime
import time
import threading
import schedule
import json
from consts import GLOBAL_OUTPUT_FILE_NAME

# file = open("first.txt", mode="a")
resent_poloniex = dict()


def on_open(ws):
    print("ON OPEN")
    ws.send(open("poloniex.json").read())


def on_message(ws, mess):
    global resent_poloniex
    mess = json.loads(mess)
    # print(mess["s"])
    resent_poloniex[mess["data"][0]["symbol"]] = (*mess["data"][0]["symbol"].split('_'), "poloniex", float(mess["data"][0]["bids"][0][0]), float(mess["data"][0]["asks"][0][0]))
    print(mess["symbol"])



def on_close(ws):
    file.close()


def run_websocket():
    print("START")

    wss = f"wss://ws.poloniex.com/ws/public"
    wsa = websocket.WebSocketApp(wss, on_message=on_message, on_open=on_open, on_close=on_close)
    wsa.run_forever()


def scheduling():
    def job():
        global resent_poloniex
        global file
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            if 'event' not in resent_poloniex.keys():
                for e in resent_poloniex.values():
                    # file.write( + "\n")
                    print("\t".join(map(str, (now_time, *e))), file=file)


    schedule.every(1).seconds.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


schedule_thread = threading.Thread(target=scheduling)
websocket_thread = threading.Thread(target=run_websocket)

all_thread = [schedule_thread, websocket_thread]
schedule_thread.start()
websocket_thread.start()