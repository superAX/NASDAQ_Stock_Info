# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:23:47 2019

@author: andyx
"""

from flask import Flask, render_template

# Establish the website object
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('./template.html')

if __name__ =="__main__":
    app.run(debug=True,port=8080)