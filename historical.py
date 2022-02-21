import requests
from requests import Request, Session
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
    #getting historical data from candles not individual trades, that might be a bit much
    pair_path = PRODUCTS + '/' + currency + '/candles'
    res = resolution
    #accept either char description or integer, but default to 1hr
    if resolution is not integer:
        if resoulution in time_lens:
            res = time_lens[resolution]
        else:
            res = time_lens['1hr'];
    if resolution not in time_lens.values():
        res = time_lens['1hr']

    #loop through provided time range given time resolution
    global progress = 0
    global threadList=[]
    callsPerSecond = 7.0
    timeToSleep = 1.0/callsPerSecond

    start = datetime.date(year, 1, 1, 0, 0, 0, 0)
    end = datetime.date(year, 12, 31, 23, 59, 59, 0)
    if year == datetime.now().year:
        end = datetime.now()

    delta = datetime.timedelta(start, end)
    deltaSeconds = delta.total_seconds()
    numCandles = int(math.ceil(deltaSeconds/(1.0*res)))
    approxCalls = int(math.ceil(numCandles/300.0))
    s = start
    e = start + delta
    while True:
        
        params = {'start':s.isoformat(), 'end':e.isoformat(), 'granularity':res}
        thread = threading.Thread(target=connect, args=(pair_path, params,), daemon=True)
        threadList.append(thread)
        thread.start()
        result_available.wait()
        if result.status_code == 200:
            filename = f'{currency}_{year}_{progress}.csv'

            df_history = pandas.read_json(result.text)
            # Add column names in line with the Coinbase Pro documentation
            df_history.columns = ['time','trade_id','price','size','side']
            
            # Index must be set as the date
            df_history.set_index('time', inplace=True)
            df_history.to_csv(f'{y_path}\\{filename}')

        else:
            print(f'skipped sequence {progress} in {currency}_{year} due to bad response')

        progress++
        progPercent = (progress/approxCalls)*100.0
        s = s+delta
        e = e+delta
        print(f'\r{progPercent}% done...  last response code:{result.status_code}', flush=True)
        print(f'')
         if exit_thread.wait(timeout=timeToSleep):
                break

    #sanity check, clean up any random threads
    for t in threadList:
        if t.isAlive():
            t.terminate()
    return True

def populateYearPath(currency, y_path, year):
    fileList = os.listdir(y_path)
    if fileList == []:
        if(getData(currency, y_path, year)):
            print(f'finished loading csv\'s for {currency} {year}')
    else:
        print(f'{currency}, {year}, contains: {fileList} from previous run. to rerun, remove old files at {y_path}')
    return True

def makeYearPaths(currency, c_path):
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
    fileList = os.listdir(c_path)
    if fileList == []:
        #super empty
        makeYearPaths(currency, c_path)
    else: 
        #possibly stopped mid DL
        for i in range (start_year, end_year+1):
            if not os.path.(c_path + f'\\{i}'):
                populateYearPath(currency, c_path + f'\\{i}', i)
         
    return True

def makeCurrencyPath(currency, c_path):
    os.mkdir(c_path)
    print(f'added {currency} subfolder')
    populateCurrencyPath(currency, c_path)
    return True



def populateRootPath():
    for currency in MY_CURRENCIES:
        curr_path = csv_path + '\\' + currency
        if os.path.isdir(curr_path) == False:
            #super empty, make the directory and fill it
            makeCurrencyPath(currency, curr_path)
        else:
            populateCurrencyPath(currency, curr_path)
    return True

def makeRootPath():
    os.mkdir(csv_path)
    print('added csv_data main folder')
    populateRootPath
    return True

def getHistorical():
    response = connect(PRODUCTS)
    response_content = response.content
    response_text = response.text
    #response_headers = response.headers
    df_currencies = pandas.read_json (response_text)
    #print("\nNumber of columns in the dataframe: %i" % (df_currencies.shape[1]))
    #print("Number of rows in the dataframe: %i\n" % (df_currencies.shape[0]))
    columns = list(df_currencies.columns)
    print('quick online status check')
    print(df_currencies[df_currencies.id.isin(MY_CURRENCIES)][['id', 'quote_currency', 'status']])
    print('quick stats')
    currency_rows = []
    for currency in MY_CURRENCIES:
        response = connect(PRODUCTS+'/'+currency+'/stats')
        response_content = response.content
        data = json.loads(response_content.decode('utf-8'))
        currency_rows.append(data)
    # Create dataframe and set row index as currency name
    df_statistics = pandas.DataFrame(currency_rows, index = MY_CURRENCIES)
    print(df_statistics)

    #check for folder structure
    if os.path.isdir(csv_path) == False:
        makeRootPath