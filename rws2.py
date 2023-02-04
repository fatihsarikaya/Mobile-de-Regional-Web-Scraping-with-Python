import bs4
from bs4 import BeautifulSoup, NavigableString, Tag
import urllib.request
from urllib import parse
import pandas as pd
from tkinter import E
import pymysql
import mysql.connector
import configparser
import re
import numpy as np
import time
import concurrent.futures
from icecream import ic
# import erequests
import lxml
from datetime import datetime, timedelta
from multiprocessing import Pool
# from multiprocessing import Process, Lock
from multiprocessing import Process
from datetime import datetime
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
    database="mobiledehandlers",
    port=3306
)
mycursor = mydb.cursor()

sql = "SELECT * FROM dealer_links"

mycursor.execute(sql)

columns = [desc[0] for desc in mycursor.description]

data_df = pd.DataFrame(mycursor.fetchall(), columns=columns)

data_dict = data_df.to_dict(orient="list")

dataframe = pd.DataFrame(data_dict)

#myresult = mycursor.fetchall()
#dealer_urls = myresult[0:]
#len_dealer_urls = len(dealer_urls)
#dataframe = pd.DataFrame(dealer_urls, columns=['dealer_url'])
#ic("dataframe:",dataframe)


x = 0
y = 1#37908

def fonksiyon(i):
    global x
    global y
#number = np.arange(x,y)
#for i in tqdm(number):
    dealer_url = dataframe.dealer_url[i]  # dealer_url = dataframe["links"][i]
    ic(dealer_url)

    dealer_name = dataframe.dealer_name[i]
    ic(dealer_name)
    dealer_adress = dataframe.dealer_adress[i]
    ic(dealer_adress)
    ad_link = dataframe.ad_link[i]
    ic(ad_link)

    fireFoxOptions = Options()
    fireFoxOptions.binary_location = "/Applications/Firefox.app/Contents/MacOS/firefox-bin"     #r'C:\Program Files\Firefox Developer Edition\firefox.exe'
    fireFoxOptions.add_argument('--disable-gpu')
    fireFoxOptions.add_argument('--no-sandbox')
    fireFoxOptions.add_argument('--headless')

    driver = webdriver.Firefox(options=fireFoxOptions)

    sleep_time = 3 

    driver.get(dealer_url)
    time.sleep(sleep_time)
    ad_source = driver.page_source
    ad_soup = BeautifulSoup(ad_source, 'lxml')

    try:
        maincnresults = ad_soup.find('div', {'class': 'count pull-right visible-desktop'})
        car_number = maincnresults.find_all('span', {'class': 'resultCount'})[0].get_text()
        ic(car_number)
    except:
        car_number = ' '

    cars_data = pd.DataFrame({
                    'car_number': car_number,
                },
                    index=[0])
    try:
        options = ad_soup.find_all('select', {'id': 'makeId'})[0].find_all('option')
        #ic(options)
        options_lenght = len(options)
        #ic(options_lenght)
    except:
        options_lenght = 0


    cars=""

    for i in range(options_lenght):
        try:
            cars = cars + " " + ad_soup.find_all('select', {'id': 'makeId'})[0].find_all('option')[i].get_text().strip()
        except:
            cars = ""
    
        cars_data['cars'] = cars[10:]
    
    #ic(cars)

    maintelnumber = ad_soup.find('div', {'class': 'phoneNumbers dealerContactPhoneNumbers'})#.get_text().strip().split('\n\n')
    
    
    try:
        for i in maintelnumber.prettify().split('<br/>')[0].split('<br/>'):
            info1_join        = str(i)# + ''.join(i)
            #ic(info1_join)
            info1_tel_replace = info1_join.replace("Tel.:\xa0+","")
            info1_gap_replace = info1_tel_replace.replace(" ","")
            info1_l_replace   = info1_gap_replace.replace("(","")
            info1_r_replace   = info1_l_replace.replace(")","")
            info1 = info1_r_replace
            #ic(info1)
            cars_data['info1'] = info1[51:66].strip()
            ic(cars_data.info1)

        for i in maintelnumber.prettify().split('<br/>')[1].split('<br/>'):
            info2_join        = str(i)# + ''.join(i)
            info2_split       = info2_join.split('+')[1]
            info2_gap_replace = info2_split.replace(" ","")
            info2_l_replace   = info2_gap_replace.replace("(","")
            info2_r_replace   = info2_l_replace.replace(")","")
            info2 = info2_r_replace
            #ic(info2) ###
            cars_data['info2'] = info2[0:15].strip() 
            ic(cars_data.info2)

        for i in maintelnumber.prettify().split('<br/>')[2].split('<br/>'):
            #info3              = str(i)# + ''.join(i)
            #cars_data['info3'] = info3[2:34].strip() 
            #ic(info3[2:34] )

            info3_join        = str(i)# + ''.join(i)
            info3_split       = info3_join.split('+')[1]
            info3_gap_replace = info3_split.replace(" ","")
            info3_l_replace   = info3_gap_replace.replace("(","")
            info3_r_replace   = info3_l_replace.replace(")","")
            info3 = info3_r_replace
            #ic(info2) ###
            cars_data['info3'] = info3[0:15].strip() 
            ic(cars_data.info3)

        for i in maintelnumber.prettify().split('<br/>')[3].split('<br/>'):
            info4_join = str(i)# + ''.join(i)
            info4 = info4_join[2:26].strip()
            if  info4[0:3] == "Fax":
                cars_data['info4'] = info4
                ic(cars_data.info4)
            else:
                cars_data['info4'] = ""
                ic(cars_data.info4)
    except:
        info1=""
        info2=""
        info3=""
        info4=""
    #for br in maintelnumber.findAll('br'):
    #    for i in range(4):
    #        next_s = br.nextSibling
    #        if not (next_s and isinstance(next_s,NavigableString)):
    #            continue
    #        next2_s = next_s.nextSibling
    #        if next2_s and isinstance(next2_s,Tag) and next2_s.name == 'br':
    #            text = str(next_s).strip()
    #            if text:
    #                print("Found:", next_s.strip())
    #                cars_data['tel' + str(i) ] = next_s.strip()
    #    i+=1


    #ic(maintelnumber)
    #mobiltelefon = maintelnumber[117:145]
    #fax  = maintelnumber[155:190]
    #contact_info = next_s.strip()

    #ic(contact_info)
    #cn = int(car_number)//2 
    #for i in range(int(car_number)):
        #maincarresultsodd  = ad_soup.find_all('li', {'class': 'listing odd'})[i].find('h3').get_text().strip()
        #maincarresultseven = ad_soup.find_all('li', {'class': 'listing even'})[i].find('h3').get_text().strip()
        #cars = maincarresultsodd + " + " + maincarresultseven ##BAK
    #cars = ad_soup.find_all('select', {'class': 'span6 hidden-phone'}).find('option').get_text().strip()

    #cars_data['contact_info'] = contact_info

    cars_data['dealer_name'] = dealer_name
    cars_data['dealer_adress'] = dealer_adress
    cars_data['ad_link'] = ad_link
    cars_data['dealer_url'] = dealer_url

    # datetime string
    datetime_string = datetime.now() + timedelta(days=0)

    cars_data['download_date_time'] = datetime_string

    #ic(cars_data)
    #df_list = cars_data.values.tolist()
    

    config = configparser.RawConfigParser()
    config.read(filenames='my.properties')

    scrap_db = pymysql.connect(host='localhost', user='root', password='', database='mobiledehandlers',port=3306,
                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    cursor = scrap_db.cursor()

    sql = """CREATE TABLE dealers_info(
        car_number VARCHAR(4),
        cars VARCHAR(500),
        info1 VARCHAR(64),
        info2 VARCHAR(64),
        info3 VARCHAR(64),
        info4 VARCHAR(64),
        dealer_name VARCHAR(64),
        dealer_adress VARCHAR(255),
        ad_link VARCHAR(64),
        dealer_url VARCHAR(255),
        download_date_time VARCHAR(32)
        )"""

    #cursor.execute(sql)   #Save data to the table

    for row_count in range(0, cars_data.shape[0]):
        chunk = cars_data.iloc[row_count:row_count + 1, :].values.tolist()

        car_number  = ' '
        cars = ""
        info1 = ""
        info2 = ""
        info3 = ""
        info4 = ""
        dealer_name = ""
        dealer_adress = ""
        ad_link = ""
        dealer_url = ""
        download_date_time = ""

        # control = "true"

        lenght_of_chunk = len(chunk[0])
        # ic("lenght_of_chunk:",lenght_of_chunk)

        if "car_number" in cars_data:
            try:
                car_number = chunk[0][0]
            except:
                car_number = ' '

        if "cars" in cars_data:
            try:
                cars = chunk[0][1]
            except:
                cars = ""

        
        if "info1" in cars_data:
            try:
                info1 = chunk[0][2]
            except:
                info1 = ""
#              
        if "info2" in cars_data:
            try:
                info2 = chunk[0][3]
            except:
                info2 = ""
#              
        if "info3" in cars_data:
            try:
                info3 = chunk[0][4]
            except:
                info3 = ""
#              
        if "info4" in cars_data:
            try:
                info4 = chunk[0][5]
            except:
                info4 = ""
#
        if "dealer_name" in cars_data:
            index_no_dealer_name = cars_data.columns.get_loc("dealer_name")
            #ic(cars_data.iloc[:,index_no_dealer_name])
            try:
                dealer_name = cars_data.iloc[:,index_no_dealer_name][0]
            except:
                dealer_name = ""
#        
        if "dealer_adress" in cars_data:
            index_no_dealer_adress = cars_data.columns.get_loc("dealer_adress")
            #ic(cars_data.iloc[:,index_no_dealer_adress])
            try:
                dealer_adress = cars_data.iloc[:,index_no_dealer_adress][0]
            except:
                dealer_adress = ""
#
        if "ad_link" in cars_data:
            index_no_ad_link = cars_data.columns.get_loc("ad_link")
            #ic(cars_data.iloc[:,index_no_ad_link])
            try:
                ad_link = cars_data.iloc[:,index_no_ad_link][0]
            except:
                ad_link = ""
#
        if "dealer_url" in cars_data:
            index_no_dealer_url = cars_data.columns.get_loc("dealer_url")
            #ic(cars_data.iloc[:,index_no_dealer_url])
            try:
                dealer_url = cars_data.iloc[:,index_no_dealer_url][0]
            except:
                dealer_url = ""
#
        if "download_date_time" in cars_data:
            index_no_download_date_time = cars_data.columns.get_loc("download_date_time")
            #ic(cars_data.iloc[:,index_no_download_date_time])
            try:
                download_date_time = cars_data.iloc[:,index_no_download_date_time][0]
            except:
                download_date_time = ""

        if (car_number == ' '):
            control = "false"
        else:
            control = "true"

        if control == "true":
            mySql_insert_query = "INSERT INTO dealers_info (car_number,cars,info1,info2,info3,info4,dealer_name,dealer_adress,ad_link,dealer_url,download_date_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val =                                          (car_number,cars,info1,info2,info3,info4,dealer_name,dealer_adress,ad_link,dealer_url,download_date_time)

            cursor = scrap_db.cursor()
            cursor.execute(mySql_insert_query, val)

            scrap_db.commit()
            ic(cursor.rowcount, "Record inserted successfully into *dealers_info* table")
        
    driver.close()

if __name__ == '__main__':
    with concurrent.futures.ProcessPoolExecutor() as executor:  #ThreadPoolExecutor
        i = list(range(x, y))  # i = [0,1,2,3...100]
        executor.map(fonksiyon, i)
