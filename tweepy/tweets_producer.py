import sys
import tweepy
import kafka
import json
import secrets

kafka_servers = ['localhost:9092']

auth = tweepy.OAuthHandler(secrets.consumer_key, secrets.consumer_secret)
auth.set_access_token(secrets.access_token, secrets.access_secret)

producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, tweet):
        print(json.dumps(tweet._json))
        producer.send('tweets-bitcoin', value=tweet._json)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)

myStream.filter(track=['bitcoin'])