
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import mysql.connector



ACCESS_TOKEN = "286251581-o6kqYi314uK9Bq4dgbPBIKAb5oki1rhA2EieYh8F"
ACCESS_TOKEN_SECRET = "7BBFqlfOHNWzAlH2mrT2VSZoIG5lsJbCAZrS4XmWwfjDq"
CONSUMER_KEY = "4dTGn91O8jztG3HENFJ4Ig9so"
CONSUMER_SECRET = "ph0a84VvkoStOpCYMMRBSW12Oyn1OsgMRnNWqLxdBwbVJHznFj"



import schedule
import time

cnx = mysql.connector.connect(user='josdead', password='12345',
                              host='34.219.19.185',
                              database='redes',
                              port=3306)



cursor = cnx.cursor()



# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)
        stream.filter(follow=["286251581", "1006634355647811584", "1006633766805229571"])

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            dic = json.loads(data)
            print(data)
            print(dic["user"]["name"])
            print(dic["user"]["created_at"])
            print(dic["text"])
            user = dic["user"]["name"]
            hora = dic["user"]["created_at"]
            texto= dic["text"]

            cursor.callproc("insertar", args=(user, hora, texto))
            cnx.commit()

            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = ["saludos","hola"]
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(hash_tag_list,hash_tag_list)