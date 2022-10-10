import tweepy
import csv
import pandas as pd
import os
# import pickle
import time
import datetime
import json
consumer_key = 'o92mtArmBga1bJCok9AKgnKy4'
consumer_secret = 'oGvSKiDAb2321bBADIuWmcg9bDBUvEpapMPXkYfmCBOHxkNRj6'
access_token = '1471322245393883142-RCb80cv7nnt8oXSDtD5nU10hf2UB15'
access_token_secret = 'PPieyPRidXYWSkDlBZSfpK3VfMh9b9DWjd8xLoXAVgRi8'
print("XXYY")
#Twitter Access
auth = tweepy.OAuthHandler( consumer_key ,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

main_directory = './data'
excel_cols=["id","text","created_at","user_screenname",'geo', 'coordinates', 'place', 'timestamp_ms']
tweet_per_file = 100

# def save_status(tweet):
#     file_path = main_directory + 'steaming/' + tweet.id_str + '_' + tag + '.pickle'
#     with open(file_path, 'wb') as f:
#         pickle.dump(tweet , f, pickle.HIGHEST_PROTOCOL)

# def save_pickle(file_path, tweets):
#     with open(file_path, 'wb') as f:
#         pickle.dump(tweets , f, pickle.HIGHEST_PROTOCOL)

def save_excel(file_path, tweets):
    tweet_list = []
    for t in tweets:
        url = 'https://twitter.com/' + t._json['user']['screen_name'] + '/status/' + t.id_str
        row = {'id':t.id_str, 
        'text' : t.text,
        'created_at' : t.created_at,
        'screenname' : t._json['user']['screen_name'],
        'geo' : t._json['geo'],
        'coordinates' : t._json['coordinates'],
        'place' : t._json['place'],
        'lang' : t._json['lang'],
        'timestamp_ms' : t._json['timestamp_ms'],
        'url' : url,
        'json' : json .dumps(t._json)}
        tweet_list.append(row)

    df = pd.DataFrame(tweet_list)
    df.to_excel(file_path , encoding='utf8')


def load_hashtags():
    filename = 'hashtags.txt'
    track = []
    with open(filename, encoding="utf8") as file:
        lines = file.readlines()
    
    for l in lines:
        if l:
            track.append(l.replace("\n","").replace("\r",""))
    return track

def getpath():
    now = datetime.datetime.now()
    year = str(now.year).zfill(4)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    hour = str(now.hour).zfill(2)
    directory = main_directory + '/steam_trends/'+ year + '/' + month + '/' + day + '/' + hour + '/'
    return directory

def get_newhashtags(directory):
    
    if not os.path.exists(directory):
        os.makedirs(directory)
        hashtags = load_hashtags()
        stream.running = False 
        stream.filter(track = hashtags)

class CustomStreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        #print(status.id_str,status.text)
        #save_status(status)
        
        self.tweets.append(status)
        #print('data collection progress #tweets  : ' , len(self.tweets) )
        if len(self.tweets) > tweet_per_file :
            directory = getpath()
            # pickle_directory = directory + 'pickle/'
            excel_directory = directory + 'excel/'
            # if not os.path.exists(pickle_directory):
            #     os.makedirs(pickle_directory)

            if not os.path.exists(excel_directory):
                os.makedirs(excel_directory)

            get_newhashtags(directory)
            currentDT = str(datetime.datetime.now()).replace('.','').replace(' ','').replace(':','')
            # pickle_file = pickle_directory + currentDT + '.pickle'
            excel_file = excel_directory  + currentDT + '.xlsx'
            # save_pickle(pickle_file, self.tweets)
            save_excel(excel_file, self.tweets)
            self.tweets = []
            print('save excel : ' , excel_file )
            #clear_output()

        def on_error(self, status_code):
            print('Encountered error with status code:', status_code)
            return True # Don't kill the stream

        def on_timeout(self):
            print ('Timeout...')
            return True # Don't kill the stream

c = CustomStreamListener()
c.tweets = []
stream = tweepy.streaming.Stream(auth, c)


while not stream.running:
    try:
        print("Started listening to twitter stream...")
        hashtags = load_hashtags()
        print("Hashtags : ", hashtags)
        stream.filter(track = hashtags, stall_warnings=True)
    except Exception as e:
        #pass
        print("Unexpected error!", e)
    finally:
        #pass
        print("Stream has crashed. System will restart twitter stream!")
    #print("Somehow zombie has escaped...!")      
