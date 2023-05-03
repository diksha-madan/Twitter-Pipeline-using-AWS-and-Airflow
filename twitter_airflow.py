import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def twitter_etl():
    access_key = "LQtFWGTO125UpN3InuVpkBixT"
    access_secret = "2iFuLLEp07RnJwUgZjvWqbCWAn1th2ag9ttwoBRn0YEEAtswBj"
    consumer_key = "1434480993495183365-vMRlZTLTcmWIyByQlGFkjAzIBAQzqH"
    consumer_secret = "ViKHq6lMvzTeT12p3CxoF9PRneMPHUdScolMHk9s0gz6P"


    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)


    api = tweepy.API(auth)

    try:
        api.verify_credentials()
        print("Authentication Successful")
    except:
        print("Authentication Error")

    tweets = api.user_timeline(screen_name='@elonmusk',
                                count=200,
                                include_rts=False,

                                tweet_mode='extended')


    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        "text": text,
                        'favorite_count':tweet.favorite_count,
                        'created_at':tweet.created_at}

        tweet_list.append(refined_tweet)


    df = pd.dataFrame(tweet_list)
    df.to_csv("s3://twitter-airflow-project/elon_musk_twitter_data.csv")
