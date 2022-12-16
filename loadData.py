# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
from hdfs import InsecureClient
client_hdfs = InsecureClient('http://localhost:9870')

df_amazon_apparel_reviews = pd.read_csv('amazon_apparel_reviews.csv')
df_amazon_automotive_reviews = pd.read_csv('amazon_automotive_reviews.csv')
df_amazon_beauty_reviews = pd.read_csv('amazon_beauty_reviews.csv')
df_amazon_books_reviews = pd.read_csv('amazon_books_reviews.csv')
df_amazon_furniture_reviews = pd.read_csv('amazon_furniture_reviews.csv')
df_amazon_grocery_reviews = pd.read_csv('amazon_grocery_reviews.csv')
df_amazon_jewelry_reivews = pd.read_csv('amazon_jewelry_reviews.csv')
df_amazon_shoes_reviews = pd.read_csv('amazon_shoes_reviews.csv')
df_amazon_toys_reviews = pd.read_csv('amazon_toys_reviews.csv')
df_app_reviews = pd.read_csv('app_reviews.csv')
df_beer_reviews = pd.read_csv('beer_reviews.csv')
df_drug_reviews = pd.read_csv('drug_reviews.csv')

with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_apparel_reviews',encoding='utf-8') as writer:
    df_amazon_apparel_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_automotive_reviews',encoding='utf-8') as writer:
    df_amazon_automotive_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_beauty_reviews',encoding='utf-8') as writer:
    df_amazon_beauty_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_books_reviews',encoding='utf-8') as writer:
    df_amazon_books_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_furniture_reviews',encoding='utf-8') as writer:
    df_amazon_furniture_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_grocery_reviews',encoding='utf-8') as writer:
    df_amazon_grocery_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_jewelry_reivews',encoding='utf-8') as writer:
    df_amazon_jewelry_reivews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_shoes_reviews',encoding='utf-8') as writer:
    df_amazon_shoes_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/amazon_toys_reviews',encoding='utf-8') as writer:
    df_amazon_toys_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/app_reviews',encoding='utf-8') as writer:
    df_app_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/beer_reviews',encoding='utf-8') as writer:
    df_beer_reviews.to_csv(writer, index = False)
    
with client_hdfs.write('/user/data/basdatpbd/dataTubes/drug_reviews',encoding='utf-8') as writer:
    df_drug_reviews.to_csv(writer, index = False)