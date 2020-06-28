import sys
import tweepy

consumer_key = None
consumer_secret = None

access_token = None
access_secret =  None

# read secrets from stdin
exec(sys.stdin.read())

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)

myStream.filter(track=['bitcoin'])