from binance.client import Client
from binance.websockets import BinanceSocketManager
import kafka
import json
import secrets
import time
import click

@click.command()
@click.option('-v', '--verbose', default=False, is_flag=True, help='Verbose mode')
@click.option('-s', '--symbol', help='Symbol to monitor', default='BTCUSDT')
@click.option('-k', '--kafka', 'kafka_servers', multiple=True, help='Kafka server. You can add more than one', default=["localhost:9092"])
@click.option('-t', '--topic', 'kafka_topic', help='Kafka topic in which to publish', default='binance-BTCUSDT')
def go(verbose, symbol, kafka_servers, kafka_topic):
    """Listen for exchange ask/bid updates and publishes them to kafka"""

    client = Client(secrets.api_key, secrets.api_secret)

    producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                            value_serializer=lambda x: 
                            json.dumps(x).encode('utf-8'))

    def process_message(msg):
        msg["timestamp"] = time.time()
        if verbose:
            print(json.dumps(msg))
        producer.send(kafka_topic, value=msg)
        # do something

    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    conn_key = bm.start_symbol_book_ticker_socket(symbol, process_message)
    # then start the socket manager
    bm.start()

if __name__ == '__main__':
    go()
