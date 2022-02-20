import requests
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas
from pandas import read_table
import time
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



def connect(url, *args):
    try:
        if args is not None:
            response = requests.get(url,args)
        else:
            response = requests.get(url)
        response.raise_for_status()
        #print('HTTP connection success!')
        return response
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

def getData(pair, start, end, resolution):
    #getting historical data from candles not individual trades, that might be a bit much
    pair_path = PRODUCTS + '/' + pair + '/candles'
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


    params = {'start':start, 'end':end, 'granularity':res}
    response = connect(pair_path, params)
    print(response.status_code)
    return response

def populateYearPath(currency, c_path, year):
    fileList = os.listdir(c_path + f'\\{i}')
    if fileList == []:
        #super empty
        response = getData(currency, start, end, TIME_SCALE)
    else:
        #see if partially filled, delete lastest (most likely to have an error) and restart from there

        print(f'{currency}, {year}, contains: {fileList}')
    return True

def makeYearPaths(currency, c_path):
    #make folders for each year starting with start year
    end_year = datetime.now().year
    for i in range (start_year, end_year+1):
        os.mkdir(c_path + f'\\{i}')
        print(f'made {i} path for {currency}')
        populateYearPath(currency, c_path, i)
        #get full data for year (or ytd for current year) and save csv(s)
    return True

def populateCurrencyPath(currency, c_path):
    if os.listdir(c_path) == []:
        #super empty
        makeYearPaths(currency, c_path)
    else:
        #check how full the subfolders are
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
            #see if we have any data 
            print(f'files in {curr_path}: ')
            print(os.listdir(curr_path))
            if os.listdir(curr_path) == []:
                #super empty, make the yearly folders and fill em
                print(f'get historical data for {currency}')
            elif CONTINUE_TRAINING:
                print('see how up to date our data is, fill in new data if wanted')
            else:
                print(f'some {currency} historical data is present, no more required at this time, edit start_year & CONTINUE_TRAINING vars if desired')
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
    

def other():
    start_date = (datetime.today() - timedelta(days=90)).isoformat()
    end_date = datetime.now().isoformat()
    # Please refer to the coinbase documentation on the expected parameters
    params = {'start':start_date, 'end':end_date, 'granularity':'86400'}
    response = connect(PRODUCTS+'/BTC-USD/candles', param = params)
    response_text = response.text
    df_history = pandas.read_json(response_text)
    # Add column names in line with the Coinbase Pro documentation
    df_history.columns = ['time','low','high','open','close','volume']
    
    # We will add a few more columns just for better readability
    df_history['date'] = pandas.to_datetime(df_history['time'], unit='s')
    df_history['year'] = pandas.DatetimeIndex(df_history['date']).year
    df_history['month'] = pandas.DatetimeIndex(df_history['date']).month
    df_history['day'] = pandas.DatetimeIndex(df_history['date']).day
    # Only display the first 5 rows
    df_history.head(5).drop(['time','date'], axis=1)
    print(df_history)

    # Make a copy of the original dataframe
    df_ohlc = df_history
    # Remove unnecessary columns and only show the last 30 days
    df_ohlc = df_ohlc.drop(['time','year','month','day'], axis = 1).head(30)
    # Columns must be in a specific order for the candlestick chart (OHLC)
    df_ohlc = df_ohlc[['date', 'open', 'high', 'low', 'close','volume']]
    # Index must be set as the date
    df_ohlc.set_index('date', inplace=True)
    # Inverse order is expected so let's reverse the rows in the dataframe
    df_ohlc = df_ohlc[::-1]
    df_ohlc.to_csv('csv_data\\ohlc.csv')
    #mpf.plot(df_ohlc,type='candle',mav=(3,6,9),volume=True)

