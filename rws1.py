import bs4
import urllib.request
from urllib import parse
import pandas as pd
from tkinter import E
import pymysql
import mysql.connector
from icecream import ic
import configparser
import re
import numpy as np
import time
import concurrent.futures
# import erequests
import lxml
from datetime import datetime, timedelta
from multiprocessing import Pool
# from multiprocessing import Process, Lock
from multiprocessing import Process
from datetime import datetime
from icecream import ic
from tqdm import tqdm  # progress bar
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.options import Options

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="mobile_de_handlers",
    port=3306
)
mycursor = mydb.cursor()

sql = "SELECT links FROM dealers_de"

mycursor.execute(sql)

myresult = mycursor.fetchall()

all_links = myresult[0:]

len_all_links = len(all_links)

dataframe = pd.DataFrame(all_links, columns=['links'])  

#ic("dataframe:",dataframe)

x = 18 #18
y = 19 #19 bolge var

#def fonksiyon(i):
    # global x
    # global y
number = np.arange(x,y) #sabit
for i in tqdm(number):

    fireFoxOptions = Options()
    fireFoxOptions.binary_location = "/Applications/Firefox.app/Contents/MacOS/firefox-bin"     #r'C:\Program Files\Firefox Developer Edition\firefox.exe'
    fireFoxOptions.add_argument('--disable-gpu')
    fireFoxOptions.add_argument('--no-sandbox')
    fireFoxOptions.add_argument('--headless')

    driver = webdriver.Firefox(options=fireFoxOptions)

    page = np.arange(0,42) # sayfa sayisi
    for k in tqdm(page):
        ad_link = dataframe.links[i] + str(k) + ".html"  # ad_link = dataframe["links"][i]
        ic(ad_link)

        sleep_time = 1

        driver.get(ad_link)
        time.sleep(sleep_time)

        ad_source = driver.page_source
        ad_soup = BeautifulSoup(ad_source, 'lxml')

        #maindiv = ad_soup.find('div', {'class': 'box'})
        #mainresults = maindiv.
        mainresults = ad_soup.find_all('div', {'class': 'dealerItem'})
        #mainresults = ad_soup.find_all('div', {'class': 'col col1'})
        #dealer = ad_soup.find_all("div", {"class": ('dealer')})
    
        n = np.arange(0,35) #sabit
        for j in tqdm(n):
            try:
                #dealer_name = dealer[j].find('a').get_text().strip()[0:60] ## buraya bak
                dealer_name = mainresults[j].find('h3').get_text()[0:250].strip()
            except:
                dealer_name = ' '
            try:
                dealer_adress = mainresults[j].get_text().split(dealer_name)[1][0:250].strip()
            except:
                dealer_adress = ' '

            try:
                dealer_url = mainresults[j].find('a').attrs['href']
                #dealer_url = dealer.find('a').attrs['href']
            except:
                dealer_url = ' '

            ic(dealer_name)
            ic(dealer_adress)
            ic(dealer_url)

            data_frame = pd.DataFrame({
                'dealer_name': dealer_name,
                'dealer_adress': dealer_adress,
                'dealer_url': dealer_url,
            },
                index=[0])
            #driver.close()
            ic(data_frame)

            #datetime string
            #now = datetime.datetime.now()
            #datetime_string = str(now.strftime("%Y%m%d_%H%M%S"))
            datetime_string = datetime.now() + timedelta(days=0)

            data_frame['ad_link'] = ad_link
            data_frame['download_date_time'] = datetime_string

            ic(datetime_string)
            ic(ad_link)

            config = configparser.RawConfigParser()
            config.read(filenames='my.properties')
            # print(config.sections())

            scrap_db = pymysql.connect(host='localhost', user='root', password='', database='mobiledehandlers',port=3306,
                                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

            cursor = scrap_db.cursor()

            sql = """CREATE TABLE dealer_links(
                dealer_name VARCHAR(64),
                dealer_adress VARCHAR(255),
                dealer_url VARCHAR(300),
                ad_link VARCHAR(64),
                download_date_time VARCHAR(64)
                )"""

            #cursor.execute(sql)   #Save data to the table

            for row_count in range(0, data_frame.shape[0]):
                chunk = data_frame.iloc[row_count:row_count + 1, :].values.tolist()

                brand_and_model = ""
                dealer_adress = ""
                dealer_url = ""
                ad_link = ""

                # control = "true"

                lenght_of_chunk = len(chunk[0])
                # print("lenght_of_chunk:",lenght_of_chunk)

                if "dealer_name" in data_frame:
                    try:
                        dealer_name = chunk[0][0]
                    except:
                        dealer_name = ""

                if "dealer_adress" in data_frame:
                    try:
                        dealer_adress = chunk[0][1]
                    except:
                        dealer_adress = ""

                if "dealer_url" in data_frame:
                    try:
                        dealer_url = chunk[0][2]
                    except:
                        dealer_url = ""

                if "ad_link" in data_frame:
                    try:
                        ad_link = chunk[0][3]
                    except:
                        ad_link = ""

                if "download_date_time" in data_frame:
                    try:
                        download_date_time = chunk[0][4]
                    except:
                        download_date_time = ""

                if (dealer_name == ' '):
                    control = "false"
                else:
                    control = "true"

                if control == "true":
                    mySql_insert_query = "INSERT INTO dealer_links (dealer_name,dealer_adress,dealer_url,ad_link,download_date_time) VALUES (%s,%s,%s,%s,%s)"
                    val = (                                         dealer_name,dealer_adress,dealer_url,ad_link,download_date_time)

                    cursor = scrap_db.cursor()
                    cursor.execute(mySql_insert_query, val)
                    #cursor.executemany(mySql_insert_query, tuple_of_tuples)

                    scrap_db.commit()
                    ic(cursor.rowcount, "Record inserted successfully into *dealer_links* table")
        
    driver.close() # bak
    #driver.quit()

#if __name__ == '__main__':
 #   with concurrent.futures.ProcessPoolExecutor() as executor:  # ThreadPoolExecutor
  #      i = list(range(x, y))  # i = [0,1,2,3...100]
   #     executor.map(fonksiyon, i)

