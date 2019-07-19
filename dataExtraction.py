# -*- coding: utf-8 -*-
"""
Project: NASDAQ Stock Info Crawler

@author: Endi Xu
"""

import pandas as pd
import csv
import sys
import time
import requests
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pymongo


# This class includes some support function
class Accessory:
    @staticmethod
    def accessDB(db_name):
        client = pymongo.MongoClient(host='localhost')
        return client[db_name]

    @staticmethod
    def logGenerator(func):
        def wrapper(*args, **kwargs):
            with open('./log.txt', 'a+') as f:
                current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                f.writelines("[%s] Call function %s\n" % (current_time, func.__name__))
            return func(*args, **kwargs)
        return wrapper


# This class is used to extract the user customized url list of NASDAQ stocks
class StockListGenerator:
    __count = 0

    # The default url list includes all stocks
    def __init__(self, symbol_list=['ALL']):
        self.symbol_list = symbol_list

    # Generate the url and name list of selected stock
    @Accessory.logGenerator
    def extract(self):
        url_list = []
        name_list = []

        try:
            db = Accessory.accessDB("NASDAQ")
        except Exception:
            # Try to download the csv file first 
            if self.__count == 0:
                print("Access Failure. One more try")
                with open('./log.txt', 'a+') as f:
                    f.writelines("    [Error] %s\n" % sys.exc_info()[0])
                self.__count += 1
                self.extract()
            else:
                print("Unable to access the database")
                with open('./log.txt', 'a+') as f:
                    f.writelines("    [Error] %s\n" % sys.exc_info()[0])
                pass
        else:
            if len(self.symbol_list) == 1 and self.symbol_list[0] == 'ALL':
                for company in db['CompanyList'].find():
                    url_list.append(company['Summary Quote'])
                    name_list.append(company['Name'])
            else:
                # Use dict to search the user specified stock symbol
                for selected in self.symbol_list:
                    each = db['CompanyList'].find_one({'Symbol': selected})
                    url_list.append(each['Summary Quote'])
                    name_list.append(each['Name'])
            return zip(url_list, name_list)

    # Update the latest company list into database
    @Accessory.logGenerator
    def updateCompanyList(self, url="https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download"):
        self.__download(url)    # Download the latest company list
        try:
            db = Accessory.accessDB("NASDAQ")
        except Exception:
            print("Unable to access the database")
            with open('./log.txt', 'a+') as f:
                f.writelines("    [Error] %s\n" % sys.exc_info()[0])
            return False
        else:
            db['CompanyList'].drop()
            with open('./companylist.csv', 'r') as csv_file:
                companys = csv.reader(csv_file)
                next(companys)    # Skip the tile row
                for company in companys:
                    company_dict = {'Symbol': company[0], 'Name': company[1], 'LastSale': company[2],
                                    'MarketCap': company[3], 'IPOyear': company[4], 'Sector': company[5],
                                    'industry': company[6], 'Summary Quote': company[7]}
                    db['CompanyList'].insert_one(company_dict)
            return True

    # Download the stock list file
    @Accessory.logGenerator
    def __download(self, url):
        try:
            r = requests.get(url)
            with open('./companylist.csv', 'wb') as f:
                f.write(r.content)
        except Exception:
            print("Unable to download.")
            with open('./log.txt', 'a+') as f:
                f.writelines("    [Error] %s\n" % sys.exc_info()[0])
        else:
            print("Download Success")


# This class is used to crawl data form NASDAQ in a async way
class AsyCrawler:
    def __init__(self, urlList):
        #self.db = Accessory.accessDB('NASDAQ')
        self.urlList = urlList
        self.output = []

    # Get the data and upload to the database
    @Accessory.logGenerator
    def getData(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.__tasksGenerator(loop))
        loop.close()
        return self.output

    # Generate a sequence of tasks for async use
    @Accessory.logGenerator
    async def __tasksGenerator(self, loop):
        async with aiohttp.ClientSession() as session:
            all_stocks = [loop.create_task(self.__loadHtml(session, url, name)) for url, name in self.urlList]
            finish, unfinish = await asyncio.wait(all_stocks)
            for each in finish:
                if each is not None:
                    #self.db['StockData'].insert_one(each.result())
                    self.output.append(each.result())

    # download the html and extract data for each stock
    @Accessory.logGenerator
    async def __loadHtml(self, session, url, name):
        try:
            r = await session.get(url)
        except Exception:
            print("Unable to connect.")
            with open('./log.txt', 'a+') as f:
                f.writelines("    [Error] %s\n" % sys.exc_info()[0])
            return []
        else:
            current_time = time.strftime("%Y/%m/%d %H:%M", time.localtime())
            stock_dict = {'Company': name, 'Symbol': url.split('/')[-1], 'Record Time': current_time}
            soup = BeautifulSoup(await r.text(), 'lxml')    # wait until the html file fully downloaded
            name, flag = " ", 0
            dataInfo = soup.find_all('div', {'class': 'column span-1-of-2'})
            for each1 in dataInfo:
                for each2 in each1.find_all('div', {'class': 'table-cell'}):
                    if flag % 2 == 0:
                        name = each2.get_text().replace(' / ', '/').replace('.', ' ').strip()
                    else:
                        stock_dict[name] = each2.get_text().replace("\xa0", " ").replace(' / ', '/').strip()
                    flag += 1
            return stock_dict
