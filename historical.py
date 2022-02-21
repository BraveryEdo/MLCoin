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

CONTINUE_TRAINING = True
REST_API = 'https://api.pro.coinbase.com'
PRODUCTS = REST_API+'/products'
csv_path = 'csv_data'
start_year = 2015
# I am only interested in a few currencies that I want to trade, so let's add them here:
#MY_CURRENCIES = ['BTC-USD','ETH-USD','LTC-USD','DOGE-USD','SHIB-USD','ALGO-USD','MANA-USD','MATIC-USD']
MY_CURRENCIES = ['BTC-USD']
TIME_SCALE = '1hr'

time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}


result=None
progress=None
threadList=[]
result_available = threading.Event()
exit_thread = threading.Event()

def connect(url, *args):
    print(f'trying {url}, {args}')
    global result
    try:
        if args is not None:
            response = requests.get(url,args)
        else:
            response = requests.get(url)
        response.raise_for_status()
        #print('HTTP connection success!')
        #return response
        result = response
        result_available.set()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
            
      

def getData(currency, y_path, year):
    print(f'getData {currency}, {year}')
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

    delta = start - end
    deltaSeconds = delta.total_seconds()
    numCandles = math.ceil(deltaSeconds/res)
    approxCalls = int(math.ceil(numCandles/300.0))
    s = start
    e = start + delta

    last_call = datetime.now()

    while True:
        
        params = {'start':s.isoformat(), 'end':e.isoformat(), 'granularity':res}
        thread = threading.Thread(target=connect, args=(pair_path, params,), daemon=True)
        threadList.append(thread)
        thread.start()
        result_available.wait()
        if result.status_code == 200:
            filename = f'{currency}_{year}_{progress}.csv'

            df_history = pandas.read_json(result.text)
            print(df_history)
            # Add column names in line with the Coinbase Pro documentation
            df_history.columns  = ['time','low','high','open','close','volume']

            # Index sorted by time

            df_history.set_index('time', inplace=True)
            df_history.to_csv(f'{y_path}\\{filename}')

        else:
            print(f'skipped sequence {progress} in {currency}_{year} due to bad response')

        progress = progress + 1
        progPercent = (progress/approxCalls)*100.0
        s = s+delta
        e = e+delta
        print(f'\r{progPercent}% done...  last response code:{result.status_code}', flush=True)
        print(f'')
        
        dt = datetime.now()-last_call
        if  dt < timeToSleep:
            print(f'dt:{dt}, target:{timeToSleep}')
            wait(min(abs(timeToSleep-dt), timeToSleep))
            
        last_call = datetime.now()

    #sanity check, clean up any random threads
    return True

def populateYearPath(currency, y_path, year):
    print("populateYearPath")
    fileList = []
    if os.path.isdir(y_path):
        fileList = os.listdir(y_path)
    if fileList == []:
        if(getData(currency, y_path, year)):
            print(f'finished loading csv\'s for {currency} {year}')
    else:
        print(f'{currency}, {year}, contains: {fileList} from previous run. to rerun, remove old files at {y_path}')
    return True

def makeYearPaths(currency, c_path):
    print("makeYearPaths")
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
    print("populateCurrencyPath")
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
            if not os.path.isdir(c_path + f'\\{i}'):
                populateYearPath(currency, c_path + f'\\{i}', i)
            else:
                pass
         
    return True

def makeCurrencyPath(currency, c_path):
    print("makeCurrencyPath")
    os.mkdir(c_path)
    print(f'added {currency} subfolder')
    populateCurrencyPath(currency, c_path)
    return True



def populateRootPath():
    print("populateRootPath")
    for currency in MY_CURRENCIES:
        curr_path = csv_path + '\\' + currency
        if os.path.isdir(curr_path) == False:
            #super empty, make the directory and fill it
            makeCurrencyPath(currency, curr_path)
        else:
            populateCurrencyPath(currency, curr_path)
    return True

def makeRootPath():
    print("makeRootPath")
    os.mkdir(csv_path)
    print('added csv_data main folder')
    populateRootPath()
    return True

def getHistorical():
    thread = threading.Thread(target=connect, args=(PRODUCTS,), daemon=True)
    threadList.append(thread)
    thread.start()
    result_available.wait()
    response_content = result.content
    response_text = result.text
    #response_headers = response.headers
    df_currencies = pandas.read_json (response_text)
    #print("\nNumber of columns in the dataframe: %i" % (df_currencies.shape[1]))
    #print("Number of rows in the dataframe: %i\n" % (df_currencies.shape[0]))
    columns = list(df_currencies.columns)
    print('quick online status check')
    print(df_currencies[df_currencies.id.isin(MY_CURRENCIES)][['id', 'quote_currency', 'status']])

    #check for folder structure
    if os.path.isdir(csv_path) == False:
        makeRootPath()
    else:
        populateRootPath()

    return True