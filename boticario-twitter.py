#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('pip', 'install tweepy')


#Import de bibliotecas necessarias para autenticação e leitura de itens do twitter


import tweepy
import sys
from pyspark.context import SparkContext
from pyspark.sql import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


# Criação de autenticação no twitter

#Itens gerados no dev team twitter omitidos do código
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

autenticator = tweepy.OAuthHandler(consumer_key, consumer_secret)
autenticator.set_access_token(access_token, access_token_secret)
api = tweepy.API(autenticator)


# Busca de tweets com palavras como Boticário e hidratante ou hidratantes

query= 'Boticário Hidratantes OR hidratante'

meus_tweets = api.search(q=query, tweet_mode = 'extended', lang='pt', count=10000)


# criação de um data frame pandas para gerar e visualizar arquivo com conteúdo solicitado


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


# Criação de sparkcontext e inicialização de spark para execução de import de CSV gerado acima com conteúdo pesquisado no twitter.


sc = SparkContext('local')
spark = SparkSession(sc)
print(type(spark))


# Import de CSV para bucket S3


from io import StringIO
import boto3

df = df_s3

bucket = 'read-boticario'
csv_buffer = StringIO()
df.to_csv(csv_buffer, sep = ';', header=False)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df_tweet.csv').put(Body=csv_buffer.getvalue())





