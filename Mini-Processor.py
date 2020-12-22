#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import csv


# In[2]:


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


# In[42]:


def mini_processor(name, tag):
    """
    reshape all csvs which generate by twitter crawler,
    :param df: a csv file
    :rtype: None
    """
    try:
        print(name)
        tweet = pd.read_csv("csv/" + name + ".csv", low_memory=False)
        retweet = pd.DataFrame({'Hit Record Unique ID': tweet["id"].tolist(),
                                "URL to article/Tweet": tweet["link"].tolist(),
                                "Source": "@" + name,
                                "Location": tweet["place"].tolist(),
                                "Hit Type": "Twitter Handle",
                                "Passed through tags": tag,
                                "Associated Publisher": np.nan,
                                "Referring Hit Record Unique ID": np.nan,
                                "Authors": tweet["name"].tolist(),
                                "Plain Text of Article or Tweet": tweet["tweet"].tolist(),  # nopep8
                                "Date": tweet["date"].tolist(),
                                "Mentions": tweet["mentions"].tolist(),
                                "Hashtags": tweet["hashtags"].tolist(),
                                "Found URL": tweet["urls"].tolist()})
        retweet.to_csv("mini/" + name + '.csv', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONNUMERIC)  # nopep8
    except(Exception):
        pass


def pre_processer(file):
    (list_name, tags) = get_list(file)
    for i in range(len(list_name)):
        txtname = list_name[i].split('@')[1]
        mini_processor(txtname, tags[i])


if __name__ == '__main__':
    pre_processer("twitter.csv")
