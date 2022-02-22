from types import NoneType
from pandas.core.base import NoNewAttributesMixin
import requests
from requests import Request, Session
from urllib.error import HTTPError
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas
from pandas import read_table
import time
import math
import threading
from datetime import datetime, timedelta
import os
from fake_useragent import UserAgent

from requests.models import PreparedRequest
CONTINUE_TRAINING = True
REST_API = 'https://api.pro.coinbase.com'
PRODUCTS = REST_API+'/products'
csv_path = 'csv_data'
start_year = 2016
# I am only interested in a few currencies that I want to trade, so let's add them here:
#MY_CURRENCIES = ['BTC-USD','ETH-USD','LTC-USD','DOGE-USD','SHIB-USD','ALGO-USD','MANA-USD','MATIC-USD']
MY_CURRENCIES = ['BTC-USD']
TIME_SCALE = '5min'

time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}
ua = UserAgent()
#print(ua.chrome)
header = {'User-Agent':str(ua.chrome)}


result=None
progress=None
threadList=[]
result_available = threading.Event()
exit_thread = threading.Event()

def connect(url, args):
    #print(f'trying {url}, {args}')
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
            
      

def getData(currency, y_path, year):
    #print(f'getData {currency} + {year} @ {y_path}')
    #getting historical data from candles not individual trades, that might be a bit much
    pair_path = PRODUCTS + '/' + currency + '/candles'
    res = time_lens[TIME_SCALE]

    #loop through provided time range given time resolution
    global progress
    progress = 0
    global threadList
    threadList = []
    callsPerSecond = 7.0
    timeToSleep = 1.0/callsPerSecond

    start = datetime(year, 1, 1, 0, 0, 0, 0)
    end = datetime(year, 12, 31, 23, 59, 59, 0)
    if year == datetime.now().year:
        end = datetime.now()
    
    candlesPerCall = 256.0 #if you change this then subsequent runs might skip current data for this year consider deleting YTD data
    delta = end - start
    deltaSeconds = delta.total_seconds()
    numCandles = math.ceil(deltaSeconds/res)
    approxCalls = int(math.ceil(numCandles/candlesPerCall))
    delta = timedelta(seconds=math.ceil(deltaSeconds/approxCalls))
    s = start
    e = start + delta

    last_call = datetime.now()

    #print(f'before while: delta:{delta}, start:{s}, end:{end}, #Calls for range:{approxCalls}, #datapoints per call:{numCandles/approxCalls}')
    global result
    while True:
        #print(f'start,end was: {start},{end}, using get with {s},{e}')
        
        filename = f'{currency}_{year}_{progress}.csv'
        finalpath = f'{y_path}\\{filename}'
        if not os.path.exists(finalpath):
            params = {'start':s.isoformat(), 'end':e.isoformat(), 'granularity':res}
            thread = threading.Thread(target=connect, args=(pair_path, params))
            threadList.append(thread)
            thread.start()
            result_available.wait()
            if result.status_code == 200:
                '''
                print(f'filename chosen: {filename}')
                print(result.__attrs__) #['_content', 'status_code', 'headers', 'url', 'history', 'encoding', 'reason', 'cookies', 'elapsed', 'request']
                print(result.status_code)
                print(result.headers)
                print(result.url)
                print(result.history)
                print(result.encoding)
                print(result.reason)
                print(result.cookies)
                print(result.elapsed)
                print(result.request)
            
                '''
                df_history = pandas.read_json(result.text)
                #print(df_history.head(5))
                # Add column names in line with the Coinbase Pro documentation
                df_history.columns  = ['time','low','high','open','close','volume']
                #add a user friendly time field, might also help in learning patterns between months
                if not 'UF_time' in df_history.columns:
                    df_history['UF_time'] = (pandas.to_datetime(df_history['time'], unit='s'))
                df_history.set_index('time', inplace=True)
                df_history.sort_index()
                df_history.to_csv(finalpath)

            else:
                print(f'skipped sequence {progress} in {currency}_{year} due to bad response')

         
            dt = (datetime.now()-last_call).total_seconds()
            if  dt < timeToSleep:
                #print(f'dt:{dt}, target:{timeToSleep}')
                time.sleep(min(abs(timeToSleep-dt), timeToSleep))
            
            last_call = datetime.now()
            #result = None 
        else:
            pass
            #no need to sleep if we're not making a request to server
            #print('file already populated')

        progress = progress + 1
        progPercent = (progress/approxCalls)*100.0
        s = s+delta
        e = e+delta
        twodecimal = "{:.2f}".format(progPercent)
        print(f'\r{currency} {year} in {TIME_SCALE} res is {twodecimal}% done...', end= '', flush=True)
        if result.response_code == 200:
            pass
        else:
            print(f'      last response not 200, instead:{result.response_code}')
        

        if not (progress < approxCalls and datetime.now()-e > timedelta(seconds=(time_lens[TIME_SCALE]*2))):
            break
    return True

def populateYearPath(currency, y_path, year):
    print(f'populateYearPath: {year} @ {y_path}')
    fileList = []
    if os.path.isdir(y_path):
        fileList = os.listdir(y_path)
    if fileList == []:
        if(getData(currency, y_path, year)):
            print(f'finished loading csv\'s for {currency} {year}')
    else:
        pass
        #print(f'{currency}, {year}, contains: {fileList} from previous run. to rerun, remove old files at {y_path}')
    return True

def makeYearPaths(currency, c_path):
    print(f'makeYearPaths {currency} @ {c_path}')
    #make folders for each year starting with start year
    end_year = datetime.now().year
    for i in range (start_year, end_year+1):
        y_path = c_path + f'\\{i}'
        os.mkdir(y_path)
        print(f'made {i} path for {currency}')
        populateYearPath(currency, y_path, i)
        #get full data for year (or ytd for current year) and save csv(s)
    return True

def populateCurrencyPath(currency, c_path):
    print(f'populateCurrencyPath {currency} @ {c_path}')
    fileList = []
    if os.path.isdir(c_path):
        fileList = os.listdir(c_path)
    if fileList == []:
        #super empty
        makeYearPaths(currency, c_path)
    else: 
        #possibly stopped mid DL
        end_year = datetime.now().year
        for i in range (start_year, end_year+1):
            y_path = c_path + f'\\{i}'
            if not os.path.isdir(y_path):
                os.mkdir(y_path)
                print(f'made {i} path for {currency}')
                populateYearPath(currency, y_path, i)
            elif os.listdir(y_path) == []:
                populateYearPath(currency, y_path, i)
            else:
                pass
                #print(f'pass case in populateCurrencyPath {currency} @ {i}')
         
    return True

def makeCurrencyPath(currency, c_path):
    print(f'makeCurrencyPath {currency} @ {c_path}')
    os.mkdir(c_path)
    print(f'added {currency} subfolder')
    populateCurrencyPath(currency, c_path)
    return True



def populateRootPath(basePath):
    print("populateRootPath")
    for currency in MY_CURRENCIES:
        curr_path = basePath + '\\' + currency
        if os.path.isdir(curr_path) == False:
            #super empty, make the directory and fill it
            makeCurrencyPath(currency, curr_path)
        else:
            populateCurrencyPath(currency, curr_path)
    return True

def makeRootPath(basePath):
    print("makeRootPath")
    os.mkdir(basePath)
    print(f'added {basePath} main folder')
    populateRootPath(basePath)
    return True

def getHistorical(basePath):
    #check for folder structure
    if os.path.isdir(basePath) == False:
        makeRootPath(basePath)
    else:
        populateRootPath(basePath)

    return True

def getHistorical():
    getHistorical(csv_path)
    return True

def getMultRes():
    for v in time_lens:
        path = f'{csv_path}_{v}_res'
        TIME_SCALE = v
        if os.path.isdir(path) == False:
            makeRootPath(path)
        else:
            populateRootPath(path)
    return True