# -*- coding: utf-8 -*-
"""
Project: NASDAQ Stock Info Crawler
@author: Endi Xu
"""

from flask import Flask, request, render_template
import dataExtraction as DE

app = Flask(__name__)


def dataGenerator(stock_list):
    url_list = DE.StockListGenerator(stock_list).extract()
    return DE.AsyCrawler(url_list).getData()

# Search user specified stock info
@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        # user raise a research request
        stock_list = []
        user_input = request.form['stockList']
        for each in user_input.split(','):
            stock_list.append(each.strip())
            crawler = dataGenerator(stock_list)
        return render_template("index.html", crawler=crawler)
    else:
        # Stay the same page, aviod 405 error
        return render_template("index.html")

# Jump to the company list website
@app.route('/companyInfo', methods=['POST', 'GET'])
def companyInfo():
    if request.method == 'POST':
        db = DE.Accessory.accessDB("NASDAQ")
        companys = db['CompanyList'].find()
        return render_template("companylist.html", companys=companys)
    else:
        return render_template("index.html")

# Update the company list
@app.route('/update', methods=['POST', 'GET'])
def update():
    if request.method == 'POST':
        flag = DE.StockListGenerator().updateCompanyList()
        return render_template("update.html", flag=flag)
    else:
        return render_template("index.html")


if __name__ == '__main__':
    app.run(debug=True)
