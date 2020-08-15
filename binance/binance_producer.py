from binance.client import Client
from binance.websockets import BinanceSocketManager
import kafka
import json
import secrets
import time

kafka_servers = ['localhost:9092']

client = Client(secrets.api_key, secrets.api_secret)

producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

def process_message(msg):
    msg["timestamp"] = time.time()
    print(json.dumps(msg))
    producer.send('binance-BTCUSDT', value=msg)
    # do something

bm = BinanceSocketManager(client)
# start any sockets here, i.e a trade socket
conn_key = bm.start_symbol_book_ticker_socket('BTCUSDT', process_message)
# then start the socket manager
bm.start()
