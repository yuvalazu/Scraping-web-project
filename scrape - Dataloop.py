import datetime
from flickrapi import FlickrAPI
import pandas as pd
import pymysql
import mysql.connector
import sqlite3
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests

# Setting Variables:

KEY = "4d1db2c8fe2e61bac942eec4a280000a"
SECRET = "7d296ab3cef913ed"

user = 'root'
passw = 'datalooppassword2011'
host = '127.0.0.1'
port = 3306
database = 'images'

SIZES = ["url_o", "url_k", "url_h", "url_l", "url_c"]


def get_photos(keyword):
    """assistance function: Gets the information from the API"""

    extras = ','.join(SIZES)
    flickr = FlickrAPI(KEY, SECRET)
    photos = flickr.walk(text=keyword,  # it will search by image title and image tags
                            extras=extras,  # get the urls for each size we want
                            privacy_filter=1,  # search only for public photos
                            per_page=50,
                            sort='relevance')  # we want what we are looking for to appear first
    return photos


def get_url(photo):
    """assistance function: extracting the url"""

    for i in range(len(SIZES)):  # makes sure the loop is done in the order we want
        url = photo.get(SIZES[i])
        if url:  # if url is None try with the next size
            return url


def scrape(keyword, size):
    """collect <size> number of images matching the <keyword> and store each image in mySQL database"""

    photos = get_photos(keyword)
    counter=0
    urls=[]

    for photo in photos:
        if counter < size:
            url = get_url(photo)  # get preffered size url
            if url:
                urls.append(url)
                counter += 1
            # if no url for the desired sizes then try with the next photo
        else:
            break

    # Create the table with urls, keyword and scrape time
    temp_df = pd.DataFrame()
    temp_df['imageUrl'] = urls
    temp_df['temp_df'] = keyword
    temp_df['scrapeTime'] = datetime.datetime.today()

    # Connect to MYSQL database:
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=passw,
                                 db=database)

    cursor = connection.cursor()

    # Insert DF to database with to_sql fun':

    temp_df.to_sql(con=connection, name='images', if_exists='append', flavor='mysql')

    # return temp_df  / for the test

def search(temp_df, minScrapeTime, maxScrapeTime, keyword, size):
    """return <size> results scrapped with <keyword> that were scraped between  <minScrapeTime> and < maxScrapeTime>"""
    connection = mysql.connector.connect(host=host,
                                         user=user,
                                         password=passw,
                                         db=database)

    mycursor = connection.cursor()

    sql_code = "SELECT * FROM images WHERE scrapeTime > minScrapeTime and scrapeTime < minScrapeTime and keyword = keyword limit size"

    mycursor.execute(sql_code)

    result = mycursor.fetchall()
    
    return result




keyword = "path"
size = 1000
print(scrape(keyword, size))




""" 
def search(temp_df, minScrapeTime, maxScrapeTime, keyword, size):
    # return <size> results scrapped with <keyword> that were scraped between  <minScrapeTime> and < maxScrapeTime>

    minidf = temp_df[(temp_df["scrapeTime"] >= minScrapeTime)&(temp_df["scrapeTime"] >= maxScrapeTime)&(temp_df["keyword"] == keyword)]
    return minidf.head(size)
"""


