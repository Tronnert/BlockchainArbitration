import websocket
import json


def on_message(ws, message):
    print(message)
    '''json_msg: object = json.loads(message)
    channel_id = int(json_msg[0])
    if (channel_id==1003) and len(json_msg)>2:
        print("volume update")
        print(json_msg[2][2])'''

def on_error(ws, error):
    print(error)

def on_close(ws, arg1, arg2):
    print("### closed ###")


def on_open(ws):
    print("ON OPEN")
    ws.send(open('post.json').read())


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.poloniex.com/ws/public",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close,
                              on_open = on_open)

    ws.run_forever()