import socket
import sys
import requests
import requests_oauthlib
import json
import re

ACCESS_TOKEN = '1179615926611845120-hK20Aep2clbxa7d0NSWnzZ8HM5okrq'
ACCESS_SECRET = 'fWMEyaPTEsHWYR6fqnKlJ7ZWqO8kzM3WNUoERabu9lKb9'
CONSUMER_KEY = 'cnRXi72VqFP3slp2hgMMbr1yN'
CONSUMER_SECRET = 'NxIywlq6kNbuhvlVYxAnxd9SL95l2rrTfj2BtIRiTg6TNLlpr7'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth,stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http, tcp_connection):
    lines=http.iter_lines(chunk_size=2048)
    for line in lines:
        # try:
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text']
        # tweet_text = re.sub('[^a-zA-z@#]', '', tweet_text)
        print("Tweet Text: " + tweet_text)
        print ("------------------------------------------")
        tcp_connection.send((tweet_text + '\n').encode("utf-8"))
        # except:
        #     e = sys.exc_info()[0]
        #     print("Error: %s" % e)

def send_tweets_to_spark(http, tcp_connection):
    lines=http.iter_lines(chunk_size=2048)
    for line in lines:
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text']
        tcp_connection.send((tweet_text + '\n').encode("utf-8"))


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
send_tweets_to_spark(resp, conn)