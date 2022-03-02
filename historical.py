from types import NoneType
from numpy import True_
from pandas.core.base import NoNewAttributesMixin
import requests
from requests import Request, Session
from urllib.error import HTTPError
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas
from pandas import read_table
import math
import threading
import time
from datetime import datetime, timedelta, timezone
import os
from fake_useragent import UserAgent
import config

REST_API = 'https://api.pro.coinbase.com'
PRODUCTS = REST_API+'/products'
csv_path = config.csv_path
start_year = config.start_year
MY_CURRENCIES = config.MY_CURRENCIES
time_lens = config.time_lens
#default time scale to use unless otherwise (correctly) specified
TIME_SCALE = '5min'

ua = UserAgent(fallback='chrome')
header = {'User-Agent':str(ua.random)}

result=None
threadList=[]
result_available = threading.Event()

'''
csv data structure:
root: .\csv_data\\
root level: csv_data\\{RESOLUTION:1min}_res:
    currency folder {CURRENCY:BTC-USD}:
        full summry file *if generated*: FULL_{BTC-USD}_{RESOLUTION}.csv
        year folder {YEAR:2020}:
            data: {BTC-USD}_{YEAR}_{sequence}.csv
            yearly data summary file *if generated*: {YEAR}_{BTC-USD}_{RESOLUTION}.csv
'''


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
    global start_year
    start_year = config.START_YEAR_BY_CURRENCY[currency]
    #print(f'getData {currency} + {year} @ {y_path}')
    #getting historical data from candles not individual trades, that might be a bit much
    pair_path = PRODUCTS + '/' + currency + '/candles'
    res = time_lens[TIME_SCALE]

    #loop through provided time range given time resolution
    file_sequence = 0
    global threadList
    threadList = []
    callsPerSecond = 5.0
    timeToSleep = 1.0/callsPerSecond
    print(f'getting data for year: {year}')
    
    start = datetime(year, 1, 1, 0, 0, 1, 0, timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, 0, timezone.utc)
   
    if year == datetime.now(timezone.utc).year:
        end = datetime.now(timezone.utc)

    if not os.listdir(y_path) == []:
        #already contains data continue to fill from last good index
        #prune useless files
        for f in os.listdir(y_path):
            f_path = f'{y_path}\\f'
            if os.path.isfile(f_path):
                #remove all 0kb files
                #remove all 0 entry
                if os.path.getsize(f_path) == 0 or len(df.read_csv(f_path), index_col=0) == 0:
                    print('removing some garb files, yw')
                    os.remove(f_path)
        #recheck to see if there is any data left after prev prune
        if not os.listdir(y_path) == []:
            #open last csv
            last_path = f'{y_path}\\{os.listdir(y_path)[-1]}'
            #make sure its not a folder
            if os.path.isfile(last_path):
                last_df = pandas.read_csv(last_path, index_col=0)
                last_index = last_df.index[-1]
                #print(f'last index: {datetime.utcfromtimestamp(last_index).isoformat()}')
                if last_index > start.timestamp() and last_index < end.timestamp():
                    #if we are more than one resolution step from end try to get more data
                    if end - datetime.utcfromtimestamp(last_index).astimezone(timezone.utc) > timedelta(seconds=time_lens[TIME_SCALE]):
                        new_start = datetime.utcfromtimestamp(last_index) 
                        #nice, actually can do something from here  
                        print(f'continuing prev DL for {currency} {year} {TIME_SCALE} at {new_start.isoformat()}')
                        start = datetime.utcfromtimestamp(last_index).astimezone(timezone.utc)
                    

    candlesPerCall = 256.0 #if you change this then subsequent runs might skip current data for this year consider deleting YTD data
    #timedelta obj which is equal to the ammount of time between 1st and last day of the year
    yeardelta = end - start
    #FIVE HUNDRED TWENTYFIVE THOUSAND SIX HUNDRED MINUTESSSSS *x60
    deltaSeconds = abs(yeardelta.total_seconds())
    #how many data points should this pull for the year
    numCandles = math.ceil(deltaSeconds/res)
    #only approximate because coinbase api might give back data slightly outside of the specified range ::shrugs::
    approxCalls = int(math.ceil(numCandles/candlesPerCall))
    #seconds between endpoints to get the correct number of candles (below a threshold)
    delta = timedelta(seconds=math.ceil(deltaSeconds/approxCalls))
    s = start
    e = start + delta


    last_call_rate_limiter = datetime.now()
    max_tries = config.max_tries
    try_count = 0
    zpad = config.csv_zero_pad_for_sequence
    #print(f'before while: delta:{delta}, start:{s}, end:{end}, #Calls for range:{approxCalls}, #datapoints per call:{numCandles/approxCalls}')
    global result
    while True:

       # print(f'using get with {s},{e} DELTA: {delta} in {year}')

        filename = f'{currency}_{year}_{str(file_sequence).zfill(zpad)}.csv'
        finalpath = f'{y_path}\\{filename}'

        last_element_timestamp = datetime.timestamp(s)
        if not os.path.exists(finalpath):
            params = {'start':s.isoformat(), 'end':e.isoformat(), 'granularity':res}
            thread = threading.Thread(target=connect, args=(pair_path, params))
            threadList.append(thread)
            thread.start()
            result_available.wait()
            if result.status_code == 200:
                try_count = 0
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
                if not len(df_history.columns) == 0:
                    df_history.columns  = ['time','low','high','open','close','volume']
                    
                    #add a user friendly time field, might also help in learning patterns between months
                    df_history.drop_duplicates(inplace=True)
                    if not 'UF_time' in df_history.columns:
                        df_history['UF_time'] = (pandas.to_datetime(df_history['time'], unit='s', utc=True))
                    df_history.set_index('time', inplace=True)
                    df_history.sort_index(inplace=True)
                    #drop all rows outside of desired year
                    df_copy = df_history
                    df_history = df_history[df_history.index >= start.timestamp()]
                    df_history = df_history[df_history.index <= end.timestamp()]
                 
                    if len(df_history) > 0:
                        last_element_timestamp = df_history.index[-1]
                        df_history.to_csv(finalpath)
                        file_sequence = file_sequence + 1
                    else:
                        #last get ended up giving 0 data
                        let_date = datetime.utcfromtimestamp(last_element_timestamp).isoformat()
                        print(f'\n returned no data after trimimming {let_date}')
                        last_element_timestamp = datetime.timestamp(datetime.utcfromtimestamp(last_element_timestamp)+timedelta(seconds=time_lens[TIME_SCALE]/2.0))
                else:
                    print(f'\rskipped sequence {file_sequence} in {currency}_{year} last response data not as expected or not present')

            else:
                try_count = try_count+1
                if try_count > max_tries:
                    print(f'\r too many bad calls, trying next frame in {currency}_{year} last response code:{result.response_code}')
                    
         
            dt = (datetime.now()-last_call_rate_limiter).total_seconds()
            if  dt < timeToSleep:
                time.sleep(min(abs(timeToSleep-dt), timeToSleep))
            
            last_call_rate_limiter = datetime.now()

        
        progPercent = 100.0-((end.timestamp()-last_element_timestamp)/(yeardelta.total_seconds())*100.0)
        s = (datetime.utcfromtimestamp(last_element_timestamp)+timedelta(seconds=time_lens[TIME_SCALE])).astimezone(timezone.utc)
        e = s+delta
        if e.year > year:
            e = end
        twodecimal = "{:.2f}".format(progPercent)
        print(f'\r{currency} {year} in {TIME_SCALE} res is {twodecimal}% done...', end= '', flush=True)
        #break cases split up for readability
        now_utc = datetime.now(timezone.utc)
        #last_element_timestamp's year is greater than target year or today

        if last_element_timestamp > end.timestamp() or last_element_timestamp > now_utc.timestamp():
            break
        #somehow start date is in the future
        elif s > e:
            break
        #start date got ahead of target year or end date before target year???
        elif s.year > year or e.year < year:
            break
        #shouldnt be reachable but yaknow
        elif e > now_utc or s > now_utc:
            breakpoint
            
    return True

def populateYearPath(currency, y_path, year):
    print(f'populateYearPath: {year} @ {y_path}')
    fileList = []
    if os.path.isdir(y_path):
        fileList = os.listdir(y_path)
    if fileList == []:
        if(getData(currency, y_path, year)):
            print(f'\nfinished loading csv\'s for {currency} {year}')
    else:
        print(f'\n{currency}, {year}, contains: {len(fileList)} files from previous run. attempting to continue')
        if(getData(currency, y_path, year)):
            print(f'\nfinished loading csv\'s for {currency} {year}')
    return True

def makeYearPaths(currency, c_path):
    global start_year
    start_year = config.START_YEAR_BY_CURRENCY[currency]
    print(f'makeYearPaths {currency} @ {c_path}')
    #make folders for each year starting with start year
    end_year = datetime.now(timezone.utc).year
    for i in range (start_year, end_year+1):
        y_path = c_path + f'\\{i}'
        os.mkdir(y_path)
        print(f'made {i} path for {currency}')
        populateYearPath(currency, y_path, i)
        #get full data for year (or ytd for current year) and save csv(s)
    return True

def populateCurrencyPath(currency, c_path):
    global start_year
    start_year = config.START_YEAR_BY_CURRENCY[currency]
    print(f'populateCurrencyPath {currency} @ {c_path}')
    fileList = []
    if os.path.isdir(c_path):
        fileList = os.listdir(c_path)
    if fileList == []:
        #super empty
        makeYearPaths(currency, c_path)
    else: 
        #possibly stopped mid DL
        end_year = datetime.now(timezone.utc).year
        for i in range (start_year, end_year+1):
            y_path = c_path + f'\\{i}'
            if not os.path.isdir(y_path):
                os.mkdir(y_path)
                print(f'made {i} path for {currency}')
                populateYearPath(currency, y_path, i)
            else:
                populateYearPath(currency, y_path, i)
         
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
        global start_year
        start_year = config.START_YEAR_BY_CURRENCY[currency]
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
    for t in time_lens:
        path = f'{csv_path}\\{t}_res'
        global TIME_SCALE
        TIME_SCALE = t
        if os.path.isdir(path) == False:
            makeRootPath(path)
        else:
            populateRootPath(path)
    print('\n\nFINISHED ALL')
    return True
