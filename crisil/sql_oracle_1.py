import argparse
import os
import json
import pymysql
import cx_Oracle 
import requests
from flask import Response
from flask import Flask
from flask import jsonify
from google.cloud import storage
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)

from flask import Flask, request, make_response, jsonify


app = Flask(__name__)


def my_sql():

    list=[]
    conn = pymysql.connect(host='sushant-crisil-test.cih0jkvggyaj.us-east-2.rds.amazonaws.com',
                            user='admin',
                            password='password',
                            database='test',
                            port=3306)
    cur = conn.cursor()
    cur.execute("SELECT * FROM entries")
    data = cur.fetchone()   
    for r in cur:
        list.append(r)
    return list


def oracle():

    list=[]
    con = cx_Oracle.connect('admin/password@sushant-oracle-db.cih0jkvggyaj.us-east-2.rds.amazonaws.com/DATABASE:1521') 
      
    cur = con.cursor() 
    cur.execute("select * from entries")
    for i in cur:
        list.append(i)
    return list


def index():
    return 'Hello World!'


def results():
    
    req = request.get_json(force=True)

    action = req.get('queryResult').get('action')

    my_sql()
    x = my_sql()

    oracle()
    y = oracle()
    
    reply = {'fulfillmentText': str(x)}

    return reply


def webhook():
    
    return make_response(jsonify(results()))


if __name__ == '__main__':
   app.run()



def hello_world(request):
    print("hiiii")
    request_json = request.get_json()
    print(request_json)
    print("byeee")
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        response_data = webhook()
        return response_data
