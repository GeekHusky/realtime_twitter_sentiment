import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = "YOURACCESSTOKEN"
ACCESS_SECRET = "YOURACCESSSECRET"
CONSUMER_KEY = "YOURCONSUMERKEY"
CONSUMER_SECRET = "YOURCONSUMERSECRET"
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            # print(full_tweet['text'])
            tweet_text = full_tweet['text']+ '\n' 

            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            # tweet_text + '\n'

            tweet_text = tweet_text.encode('utf-8') # pyspark can't accept stream, add '\n'
            tcp_connection.send(tweet_text)
        except:
            e = sys.exc_info()[0]
            print("stts Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'),('track','Toronto')] # language english & keyword Toronto
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)



