# -*- coding: utf-8 -*-
"""
Project: NASDAQ Stock Info Crawler

@author: Endi Xu
"""

import pandas as pd
import sys
import requests
import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup

"""
This class is used to extract the user customized url list of NASDAQ stocks
The default url list includes all stocks 
"""
class StockListGenerator: 
    count = 0
    
    def __init__(self, symbolList = ['ALL']) :
        self.symbolList = symbolList
    
    def extract(self) :
        urlList = []                          
        nameList = []
        
        try :
            stock_data = pd.read_csv('./companylist.csv', header = None)    # Load the stock info from the csv file
        except :
            # Try to download the csv file first 
            if self.count == 0 :
                print("File companylist.csv is missing or damaged. Try to download...")
                self.download()
                self.count += 1
                self.extract()
            else :
                print(sys.exc_info()[0])
                pass
        else :
            if len(self.symbolList) == 1 and self.symbolList[0] == 'ALL' :
                return zip(stock_data.iloc[1:,7].values.tolist(), stock_data.iloc[1:,1].values.tolist())                      
        
            # Use dict to search the user specified stock symbol
            stock_data_dict = stock_data.set_index(0).T.to_dict('list')
            for selected in self.symbolList :
                if selected in stock_data_dict :
                    urlList.append(str(stock_data_dict[selected][-2]))
                    nameList.append(str(stock_data_dict[selected][0]))
            return zip(urlList, nameList)
    
    
    # This function is used to download the stock list file      
    def download(self, url = "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download") :
        try :
            r = requests.get(url)
            with open('./companylist.csv', 'wb') as f :
                f.write(r.content)
        except :
            print("Unable to download.")
            print(sys.exc_info()[0])
        else :
            print("Download Success")

"""
This class is used to download user specified stocks info and generate a dataframe type output
"""
class Crawler:        
    def __init__(self, targetList) :
        self.targetList = targetList
    
    # If csvFlag is true, this function will generate a .csv file in the end 
    def getData(self, csvFlag = True) :
        # The dataframe which is used to store info for each stock
        stockDF = pd.DataFrame(columns = ('Company', 'Symbol', 'Best Bid/Ask', '1 Year Target', 'Today\'s High/Low', \
                                          'Share Volume', '50 Day Avg. Daily Volume', 'Previous Close', '52 Week High/Low', \
                                          'Market Cap', 'P/E Ratio', 'Forward P/E (1y)', 'Earnings Per Share (EPS)', \
                                          'Annualized Dividend', 'Ex Dividend Date', 'Dividend Payment Date', 'Current Yield', \
                                          'Beta'))
        
        for url, name in self.targetList :
            dataDict = {}                     # Use dict to store info and add to the dataframe
            content = self.__loadHtml(url)    # Load the html for each stock    
            if content == None :              # skip if the html file is not correctly downloaded
                continue
            soup = BeautifulSoup(content, 'lxml')
            dataDict['Company'], dataDict['Symbol']  = name, url.split('/')[-1]
            name, flag = " ", 0
            dataInfo = soup.find_all('div', {'class':'column span-1-of-2'})
            for each1 in dataInfo :
                for each2 in each1.find_all('div', {'class': 'table-cell'}) : 
                    if (flag%2 == 0) :
                        name = each2.get_text().replace(' / ', '/').strip()
                    else :
                        dataDict[name] = each2.get_text().replace("\xa0", " ").replace(' / ', '/').strip()
                    flag += 1
            stockDF = stockDF.append(dataDict, ignore_index = True)
        if (csvFlag) : 
            timeStr = time.strftime("%Y-%m-%d-%H-%M", time.localtime())
            stockDF.to_csv("report/" + timeStr + ".csv", index=False)
        return stockDF
    
    # download the html for each stock    
    def __loadHtml(self, url) :
        try :
            r = requests.get(url)  
        except :
            print("Unable to connect.")
            print(sys.exc_info()[0])            
        else :    
            return r.text
    
"""
This class performs the same function as Class 'Crawler' but uses async way instead.
The speed is faster than Crawler when num of stocks is very large
"""
class AsyCrawler:        
    def __init__(self, urlList) :
        self.urlList = urlList
    
    # If csvFlag is true, this function will generate a .csv file in the end 
    async def getData(self, loop, csvFlag = True) :        
        # The dataframe which is used to store info for each stock
        stockDF = pd.DataFrame(columns = ('Company', 'Symbol', 'Best Bid/Ask', '1 Year Target', 'Today\'s High/Low', \
                                          'Share Volume', '50 Day Avg. Daily Volume', 'Previous Close', '52 Week High/Low', \
                                          'Market Cap', 'P/E Ratio', 'Forward P/E (1y)', 'Earnings Per Share (EPS)', \
                                          'Annualized Dividend', 'Ex Dividend Date', 'Dividend Payment Date', 'Current Yield', \
                                          'Beta'))
        
        async with aiohttp.ClientSession() as session:
            allStocks = [loop.create_task(self.__loadHtml(session, each))for each in self.urlList]
            finish, unfinish = await asyncio.wait(allStocks)
            for each in finish :
                if each != None :
                    stockDF = stockDF.append(each.result(), ignore_index = True) 
        if csvFlag : 
           timeStr = time.strftime("%Y-%m-%d-%H-%M", time.localtime())
           stockDF.to_csv("report/" + timeStr + ".csv", index=False)
        return stockDF
    
    # download the html and extract data for each stock    
    async def __loadHtml(self, session, url) :
        try :
            r = await session.get(url)
        except :
            print("Unable to connect.")
            print(sys.exc_info()[0])            
        else :    
            dataDict = {}                                    # Use dict to store info and add to the dataframe
            soup = BeautifulSoup(await r.text(), 'lxml')     # wait until the html file fully downloaded
            dataDict['Company'], dataDict['Symbol']  = soup.h1.get_text().split("Common Stock")[0].strip(), url.split('/')[-1]
            name, flag = " ", 0
            dataInfo = soup.find_all('div', {'class':'column span-1-of-2'})
            for each1 in dataInfo :
                for each2 in each1.find_all('div', {'class': 'table-cell'}) : 
                    if (flag%2 == 0) :
                        name = each2.get_text().replace(' / ', '/').strip()
                    else :
                        dataDict[name] = each2.get_text().replace("\xa0", " ").replace(' / ', '/').strip()
                    flag += 1
            return dataDict