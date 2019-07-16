# -*- coding: utf-8 -*-
"""
Project: NASDAQ Stock Info Crawler

@author: Endi Xu
"""

from flask import Flask, request, render_template
import dataExtraction as de

app = Flask(__name__) 

# Search user specified stock info
@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST' :
        # user raise a rearsh request
        stockList = []
        userInput = request.form['stockList']    
        for each in userInput.split(',') :
            stockList.append(each.strip())
        urlList = de.StockListGenerator(stockList).extract()
        crawler = de.Crawler(urlList).getData().values.tolist()
        return render_template("index.html", crawler=crawler)  
    else :
        # Stay the same page, aviod 405 error
        return render_template("index.html")  

# Jump to the company list website
@app.route('/companyInfo', methods=['POST', 'GET'])
def companyInfo() :
    if request.method == 'GET' :
        return render_template("companylist.html")
    else :
        return render_template("index.html")

if __name__ == '__main__':
    app.run(debug=True)