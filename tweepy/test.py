import sys
import tweepy
import kafka
import json

kafka_servers = ['localhost:9092']

consumer_key = None
consumer_secret = None

access_token = None
access_secret =  None

# read secrets from stdin
exec(sys.stdin.read())

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

producer = kafka.KafkaProducer(bootstrap_servers=kafka_servers,
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)
        producer.send('test', value=status.text)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)

myStream.filter(track=['bitcoin'])