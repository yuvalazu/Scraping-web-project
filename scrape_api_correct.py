from flickrapi import FlickrAPI
import pandas as pd
import datetime
import mysql.connector

# Setting Variables:

KEY = "4d1db2c8fe2e61bac942eec4a280000a"
SECRET = "7d296ab3cef913ed"
host = "localhost"
user = "yuv"
passwd = "yuvADI2509#"
database = "test"
auth_plugin ='mysql_native_password'
SIZES = ["url_o", "url_k", "url_h", "url_l", "url_c"]


def get_photos(keyword):
    """assistance function: Gets the information from the API"""

    extras = ','.join(SIZES)
    flickr = FlickrAPI(KEY, SECRET)
    photos = flickr.walk(text=keyword,  # it will search by image title and image tags
                         extras=extras,  # get the urls for each size we want
                         privacy_filter=1,  # search only for public photos
                         sort='relevance')  # we want what we are looking for to appear first
    return photos


def get_url(photo):
    """assistance function: extracting the url"""

    for i in range(len(SIZES)):
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
    temp_df['scrapeTime'] = datetime.datetime.today()
    temp_df['keyword'] = keyword

    # Connect to MYSQL database:
    connection = mysql.connector.connect(host=host,
                                         user=user,
                                         password=passwd,
                                         db=database)
    cursor = connection.cursor()

    # Insert DF to database:
    data_insert = temp_df.values.tolist()
    sql = "INSERT INTO images (imagesUrl,scrapeTime,keyword) VALUES (%s,%s,%s)"
    cursor.executemany(sql, data_insert)
    connection.commit()

    # return temp_df


def search(minScrapeTime, maxScrapeTime, keyword, size):
    """return <size> results scrapped with <keyword> that were scraped between  <minScrapeTime> and < maxScrapeTime>"""
    connection = mysql.connector.connect(host=host,
                                         user=user,
                                         password=passwd,
                                         db=database)
    cursor = connection.cursor()

    sql_code = "SELECT * FROM images"
    cursor.execute(sql_code)

    result = cursor.fetchall()
    column = ["imagesUrl", "scrapeTime", "keyword"]
    df = pd.DataFrame(result, columns=column)
    df_filter = df[(df["scrapeTime"] >= minScrapeTime) & (df["scrapeTime"] <= maxScrapeTime) & (df["keyword"] == keyword)]
    return df_filter.head(size)



keyword = "sun"
size = 50
scrape(keyword, size)
minScrapeTime= "2022-11-22 14:57:57"
maxScrapeTime= "2022-11-25 09:57:57"
print(search(minScrapeTime, maxScrapeTime, keyword, size))