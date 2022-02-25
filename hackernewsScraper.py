import requests
from requests import Request, Session
from urllib.error import HTTPError
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas
from pandas import read_table
import threading
import time
from datetime import datetime, timedelta, timezone
import os
from fake_useragent import UserAgent
import config

base_path = config.sentiment_data_path
raw_data_path = base_path +'\\raw_data'
processed_data_path = base_path + '\\processed_data'


ycombinator = https://news.ycombinator.com
ua = UserAgent(fallback='chrome')
header = {'User-Agent':str(ua.random)}

result=None
threadList=[]
result_available = threading.Event()

max_tries=config.max_tries

def connect(url, args):
    global result
    response = None
    try:
        if args is not None:
            response = requests.get(url, args, headers=header)
        else:
            response = requests.get(url)
        response.raise_for_status()
        #print('HTTP connection success!')
        result = response
        result_available.set()
        return response
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

