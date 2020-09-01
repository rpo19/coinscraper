import sys
import tweepy
import kafka
import json
import secrets
import click

@click.command()
@click.option('-v', '--verbose', default=False, is_flag=True, help='Verbose mode')
@click.option('-f', '--filter', 'tweets_filter', multiple=True, help='Filter tweets. You can add more than one', default=["bitcoin"])
@click.option('-k', '--kafka', 'kafka_servers', multiple=True, help='Kafka server. You can add more than one', default=["localhost:9092"])
@click.option('-t', '--topic', 'kafka_topic', help='Kafka topic in which to publish', default='tweets-bitcoin')
def go(verbose, tweets_filter, kafka_servers, kafka_topic):
    """Listen for tweets and publishes them to kafka"""

    auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
    auth.set_access_token(secrets.access_token, secrets.access_secret)

    producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                            value_serializer=lambda x: 
                            json.dumps(x).encode('utf-8'))

    class TweetsStreamListener(tweepy.StreamListener):

        def on_status(self, tweet):
            if verbose:
                print(json.dumps(tweet._json))
            producer.send(kafka_topic, value=tweet._json)

    tweetsStreamListener = TweetsStreamListener()
    twStream = tweepy.Stream(auth = auth, listener=tweetsStreamListener)

    twStream.filter(track=tweets_filter)

if __name__ == '__main__':
    go()