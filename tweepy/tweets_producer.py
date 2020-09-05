import sys
import tweepy
import kafka
import json
import secrets
import click
import sys
import time

@click.command()
@click.option('-v', '--verbose', default=False, is_flag=True, help='Verbose mode')
@click.option('-f', '--filter', 'tweets_filter', multiple=True, help='Filter tweets. You can add more than one', default=["bitcoin"])
@click.option('-k', '--kafka', 'kafka_servers', multiple=True, help='Kafka server. You can add more than one', default=["localhost:9092"])
@click.option('-t', '--topic', 'kafka_topic', help='Kafka topic in which to publish', default='tweets-bitcoin')
def go(verbose, tweets_filter, kafka_servers, kafka_topic):
    """Listen for tweets and publishes them to kafka"""

    auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.access_secret)

    for i in range(0,10):
        try:
            producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                                    value_serializer=lambda x: 
                                    json.dumps(x).encode('utf-8'))
            break
        except:
            print("ERROR while trying to connecting to kafka")
            time.sleep(2)
            if i == 9:
                sys.exit(1)

    class TweetsStreamListener(tweepy.StreamListener):

        def on_status(self, tweet):
            if verbose:
                print(json.dumps(tweet._json))
            msg = tweet._json
            msg["receivedat"] = time.time()
            producer.send(kafka_topic, value=msg)

    tweetsStreamListener = TweetsStreamListener()
    twStream = tweepy.Stream(auth = auth, listener=tweetsStreamListener)

    twStream.filter(track=tweets_filter)

if __name__ == '__main__':
    go()