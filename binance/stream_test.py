import sys
from getpass import getpass
if sys.stdin.isatty():
    PUBLIC = getpass(prompt="PUBLIC: ")
    SECRET = getpass(prompt="SECRET: ")
else:
    PUBLIC = input()
    SECRET = input()

import time
from binance.client import Client # Import the Binance Client
from binance.websockets import BinanceSocketManager # Import the Binance Socket Manager
from twisted.internet import reactor

# Instantiate a Client 
client = Client(api_key=PUBLIC, api_secret=SECRET)

# Instantiate a BinanceSocketManager, passing in the client that you instantiated
bm = BinanceSocketManager(client)

# This is our callback function. For now, it just prints messages as they come.
def handle_message(msg):
    print(msg)

# Start trade socket with 'ETHBTC' and use handle_message to.. handle the message.
conn_key = bm.start_aggtrade_socket('BTCUSDT', handle_message)
# conn_key = bm.start_book_ticker_socket(handle_message)
# conn_key = bm.start_ticker_socket(handle_message)
# then start the socket manager
bm.start()

# let some data flow..
time.sleep(10)
print("started")

# stop the socket manager
bm.stop_socket(conn_key)
print("stopped socket")

bm.close()
print("closed")

reactor.stop()