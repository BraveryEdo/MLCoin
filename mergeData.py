import pandas
import time
import datetime
import os


time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}

#deprecated cuz its now included in get historical data
def addReadableTime():

    for t in time_lens:
        path = f'csv_data_{t}_res'
        #print(path)
        if os.path.isdir(path):
            if not os.listdir(path) == []:
                for c in os.listdir(path):
                    currency_path = f'{path}\\{c}'
                    #print(currency_path)
                    if os.path.isdir(currency_path):
                        if not os.listdir(currency_path) == []:
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                #print(y_path)
                                if not os.listdir(y_path) == []:
                                    for f in os.listdir(y_path):
                                        #print(f)
                                        f_path = f'{y_path}\\{f}'
                                        #print(f_path)
                                        if os.path.exists(f_path):
                                            df = pandas.read_csv(f_path)
                                            if not 'UF_time' in df.columns:
                                                df['UF_time'] = (pandas.to_datetime(df['time'], unit='s'))
                                                df.to_csv(f_path)
#also deprecated for same reason as above 
def setIndex():
    for t in time_lens:
        path = f'csv_data_{t}_res'
        #print(path)
        if os.path.isdir(path):
            if not os.listdir(path) == []:
                for c in os.listdir(path):
                    currency_path = f'{path}\\{c}'
                    #print(currency_path)
                    if os.path.isdir(currency_path):
                        if not os.listdir(currency_path) == []:
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                #print(y_path)
                                if not os.listdir(y_path) == []:
                                    for f in os.listdir(y_path):
                                        #print(f)
                                        f_path = f'{y_path}\\{f}'
                                        #print(f_path)
                                        if os.path.exists(f_path):
                                            df = pandas.read_csv(f_path)
                                            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                                            df.set_index('time', inplace=True)
                                            df.sort_index()
                                            df.to_csv(f_path)






def merger():
    for t in time_lens:
        path = f'csv_data_{t}_res'
        #print(path)
        if os.path.isdir(path):
            if not os.listdir(path) == []:
                for c in os.listdir(path):
                    currency_path = f'{path}\\{c}'
                    #print(currency_path)
                    currencydf = pandas.DataFrame({})
                    if os.path.isdir(currency_path):
                        if not os.listdir(currency_path) == []:
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                #print(y_path)
                                yearlydf = pandas.DataFrame({})
                                if not os.listdir(y_path) == []:
                                    for f in os.listdir(y_path):
                                        #print(f)
                                        f_path = f'{y_path}\\{f}'
                                        df = pandas.read_csv(f_path)
                                        
                                        if yearlydf.empty:
                                            yearlydf = df
                                        else:
                                            yearlydf = pandas.concat([yearlydf, df])

                                    yearlydf = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                                    yearlydf.set_index('time', inplace=True)
                                    yearlydf.sort_index()

                                    yearlydf.to_csv(f'{y_path}\\{y}_{c}_{t}.csv')
                                if currencydf.empty:
                                    print(f'first {y} yearly for {c}_{t}')
                                    currencydf = yearlydf
                                else:
                                    print(f'merger yearly adding on  {y}_{c}_{t}')
                                    pandas.concat([currencydf, yearlydf])
                            currencydf = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                            currencydf.set_index('time', inplace=True)
                            currencydf.sort_index()
                            print(f'finished full merge for {c} {t}')
                            currencydf.to_csv(f'{currency_path}\\FULL_{c}_{t}.csv')
                                         