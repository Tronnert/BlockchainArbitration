import websocket
from datetime import datetime
import time
import threading
import schedule
import json
from consts import GLOBAL_OUTPUT_FILE_NAME


#file = open("first.txt", mode="a")
resent = dict()

def on_open(ws):
    print("ON OPEN")
    ws.send(open("binance_sub.json").read())


def on_message(ws, mess):
    global resent
    mess = json.loads(mess)
    #print(mess["s"])
    resent[mess["s"]] = (mess["s"], "binance", float(mess["b"]), float(mess["a"]))
    # print(1)


def on_close(ws):
    file.close()


def run_websocket():
    print("START")
    
    stream = "tronnert_stream"
    wss = f"wss://stream.binance.com:9443/ws/{stream}"
    wsa = websocket.WebSocketApp(wss, on_message=on_message, on_open=on_open, on_close=on_close)
    wsa.run_forever()

def scheduling():
    def job():
        global resent
        global file
        now_time = int(datetime.utcnow().timestamp())
        with open(GLOBAL_OUTPUT_FILE_NAME, mode="a") as file:
            for e in resent.values():
                #file.write( + "\n")
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
