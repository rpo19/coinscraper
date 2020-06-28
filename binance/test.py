from binance.client import Client
from binance.websockets import BinanceSocketManager


client = Client(api_key, api_secret)

def process_message(msg):
    print("message type: {}".format(msg['e']))
    print(msg)
    # do something

bm = BinanceSocketManager(client)
# start any sockets here, i.e a trade socket
conn_key = bm.start_trade_socket('BNBBTC', process_message)
# then start the socket manager
bm.start()
