#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('pip', 'install tweepy')


# In[ ]:


import tweepy
import sys
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


# In[ ]:


consumer_key = 'sj0l73D6zc6DWi4hg1RStup6T'
consumer_secret = 'OUv8tbn2On6HWTKXNWxdkyRkH6fEvSyVRNh2eHt1jOhOug9MrH'
access_token = '1115032444682502144-lB9B5XcOjWhhRDcET2lvrqayudPsnM'
access_token_secret = 'myKZ6gWeYEi0p86i9lYYphjq7iRteT4AK665VsglEmtlu'

autenticator = tweepy.OAuthHandler(consumer_key, consumer_secret)
autenticator.set_access_token(access_token, access_token_secret)
api = tweepy.API(autenticator)


# In[ ]:


query= 'Boticário Hidratantes OR hidratante'

meus_tweets = api.search(q=query, tweet_mode = 'extended', lang='pt', count=10000)


# In[ ]:


import pandas as pd

dados = []
counter = 0
for tweet in meus_tweets:
    conteudo = {'DT_TWEET' : tweet.created_at, 'USUARIO' : tweet.user.name, 'ORIGEM' : tweet.source, 'TWEET' : tweet.full_text.replace('\n', ' ').replace('\r', '')}
    dados.append(conteudo) 
    
    counter=counter+1
    if counter==50:
      break    

df_s3 = pd.DataFrame(dados)
display(df_s3)


# In[ ]:


sc = SparkContext('local')
spark = SparkSession(sc)
print(type(spark))


# In[ ]:


from io import StringIO
import boto3


# In[ ]:


df = df_s3

bucket = 'read-boticario'
csv_buffer = StringIO()
df.to_csv(csv_buffer, sep = ';', header=False)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df_tweet.csv').put(Body=csv_buffer.getvalue())


# In[ ]:




