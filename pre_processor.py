#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np


def get_list(file):
    """
    get twitter user from csv and return a list ie["@aaa","@bbb","@ccc"]
    :param df: a csv file
    :rtype: a list
    """
    try:
        # try to read csv file
        df = pd.read_csv(file)
        name_list = df["Source"].tolist()
        tags = df["Tags"].tolist()
        return (name_list, tags)
    except(Exception):
        print("Input file is not csv or doesn't exist such csv")
        exit(1)


def pre_processor(file):
    """
    reshape and combine all csvs which generate by twitter crawler,
    the combination of csv store as one file and named final.csv
    :param df: a csv file
    :rtype: None
    """
    # create an empty dataframe for final output
    header_list = ['Hit Record Unique ID', 
                   "URL to article/Tweet", 
                   "Source",
                   "Location",
                   "Hit Type",
                   "Passed through tags",
                   "Associated Publisher",
                   "Referring Hit Record Unique ID",
                   "Authors",
                   "Plain Text of Article or Tweet",
                   "Date",
                   "Mentions",
                   "Hashtags",
                   "Found URL"]

    Final = pd.DataFrame(columns=header_list)
    # get the sourse list and tags list
    (list_name, tags) = get_list("twitter.csv") 
    # reshape each twitter user's tweets output and add it into final dataframe
    for i in range(len(list_name)):
        txtname = list_name[i].split('@')[1]
        try:
            print(txtname)
            tweet = pd.read_csv("csv/" +txtname+".csv", low_memory=False)
            print(len(tweet))
            retweet = pd.DataFrame({'Hit Record Unique ID': tweet["id"].tolist(),
                                    "URL to article/Tweet": tweet["link"].tolist(),
                                    "Source": list_name[i],
                                    "Location": tweet["place"].tolist(),
                                    "Hit Type": "Twitter Handle",
                                    "Passed through tags": tags[i],        
                                    "Associated Publisher": np.nan,
                                    "Referring Hit Record Unique ID": np.nan,
                                    "Authors": tweet["name"].tolist(),
                                    "Plain Text of Article or Tweet": tweet["tweet"].tolist(),
                                    "Date": tweet["date"].tolist(),
                                    "Mentions": tweet["mentions"].tolist(),
                                    "Hashtags": tweet["hashtags"].tolist(),
                                    "Found URL": tweet["urls"].tolist()})
            Final = Final.append(retweet,sort=False)
        except(Exception):
            pass
    # store dataframe as csv
    Final.to_csv('final.csv', index=False, encoding='utf-8-sig')    


if __name__ == '__main__':
    header_list = ['Hit Record Unique ID', 
                   "URL to article/Tweet", 
                   "Source",
                   "Location",
                   "Name",
                   "Hit Type",
                   "Passed through tags",
                   "Associated Publisher",
                   "Referring Hit Record Unique ID",
                   "Authors",
                   "Plain Text of Article or Tweet",
                   "Date",
                   "Mentions",
                   "Hashtags",
                   "Found URL"]

    Final = pd.DataFrame(columns=header_list)



(list_name, tags) = get_list("twitter.csv") 

dt_set = {'id': int,                         
          'conversation_id': int,
          'created_at': object,
          'date': object,
          'time': object,
          'timezone': int,
          'user_id': int,
          'username': object,
          'name': object,
          'place': float,
          'tweet': object,
          'language': object,
          'mentions': object,
          'urls': object,
          'photos': object,
          'replies_count': int,
          'retweets_count': int,
          'likes_count': int,
          'hashtags': object,      
          'cashtags': object,
          'link': object,
          'retweet': float,
          'quote_url': float,
          'video': int,
          'thumbnail': float,
          'near': float,
          'geo': float,
          'source': float,
          'user_rt_id': float,
          'user_rt': float,
          'retweet_id': float,
          'reply_to': object,
          'retweet_date': float,
          'translate': float,
          'trans_src': float,
          'trans_dest': float}


# In[8]:


for i in range(len(list_name)):
    txtname = list_name[i].split('@')[1]
    try:
        print(txtname)
        tweet = pd.read_csv("csv/" +txtname+".csv", low_memory=False)
        print(len(tweet))
        retweet = pd.DataFrame({'Hit Record Unique ID': tweet["id"].tolist(),
                                "URL to article/Tweet": tweet["link"].tolist(),
                                "Source": list_name[i],
                                "Location": tweet["place"].tolist(),
                                "Hit Type": "Twitter Handle",
                                "Passed through tags": tags[i],        
                                "Associated Publisher": np.nan,
                                "Referring Hit Record Unique ID": np.nan,
                                "Authors": tweet["name"].tolist(),
                                "Plain Text of Article or Tweet": tweet["tweet"].tolist(),
                                "Date": tweet["date"].tolist(),
                                "Mentions": tweet["mentions"].tolist(),
                                "Hashtags": tweet["hashtags"].tolist(),
                                "Found URL": tweet["urls"].tolist()})
        Final = Final.append(retweet, sort=False)
    except(Exception):
        pass


Final.to_csv('final.csv', index=False, encoding='utf-8-sig')
Final
