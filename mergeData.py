import pandas
import os
import config

start_year = config.start_year
time_lens = config.time_lens
csv_path = config.csv_path

'''
csv data structure:
workspace:
root level: [csv_data_{RESOLUTION}_res]:
    currency folder [BTC-USD]:
        full summry file *if generated*: FULL_[BTC-USD]_{RESOLUTION}.csv
        year folder [2020]:
            data: [BTC-USD]_{YEAR}_{sequence}.csv
            yearly data summary file *if generated*: {YEAR}_[BTC-USD]_{RESOLUTION}.csv
'''
'''
#deprecated cuz its now included in get historical data
def addReadableTime():

    for t in time_lens:
        path = f'{csv_path}_{t}_res'
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
                                            df = pandas.read_csv(f_path, index_col=0)
                                            if not 'UF_time' in df.columns:
                                                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                                                #df.set_index('time', inplace=True)
                                                df['UF_time'] = (pandas.to_datetime(df['time'], unit='s'))
                                                df.drop_duplicates(inplace=True)
                                                df.to_csv(f_path)
#also deprecated ... baked into get historical data
def setIndex():
    for t in time_lens:
        path = f'{csv_path}_{t}_res'
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
                                            df = pandas.read_csv(f_path, index_col=0)
                                            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                                            #df.set_index('time', inplace=True)
                                            df.drop_duplicates(inplace=True)
                                            df.sort_index(inplace=True)
                                            df.to_csv(f_path)
'''

def merger():
    for t in time_lens:
        path = f'{csv_path}_{t}_res'
        print(path)
        if os.path.isdir(path):
            if not os.listdir(path) == []:
                for c in os.listdir(path):
                    currency_path = f'{path}\\{c}'
                    print(currency_path)
                    full_name = f'{currency_path}\\FULL_{c}_{t}.csv'
                    if os.path.isdir(currency_path):
                        if not os.listdir(currency_path) == []:
                            
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                print(y_path)
                                if (not full_name == y_path) and (os.path.isdir(y_path)):
                                    
                                    all_files_for_year = []
                                    yearlyName = f'{y_path}\\{y}_{c}_{t}.csv'

                                    if not os.listdir(y_path) == []:
                                        for f in os.listdir(y_path):
                                            print(f'\r{f}', end='', flush=True)
                                            f_path = f'{y_path}\\{f}'
                                            if not f_path == yearlyName:
                                                df = pandas.read_csv(f_path, index_col=0)
                                                all_files_for_year.append(df)

                                        yearlydf = pandas.concat(all_files_for_year)
                                        yearlydf = yearlydf.loc[:, ~yearlydf.columns.str.contains('^Unnamed')]
                                        #yearlydf.set_index('time', inplace=True)
                                        yearlydf.drop_duplicates(inplace=True)
                                        yearlydf.sort_index(inplace=True)
                                        yearlydf.to_csv(yearlyName)
                                        print(f'\rfinished writing {yearlyName}')
                                    else:
                                         print(f'no files present to merge in {y_path}')

                            
                            all_yearly_files_for_currency = []
                            
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                if os.path.isdir(y_path):
                                    yearly = f'{y_path}\\{y}_{c}_{t}.csv'
                                    if os.path.exists(yearly):
                                        df = pandas.read_csv(yearly, index_col = 0)
                                        all_yearly_files_for_currency.append(df)
                                   
                            if not all_yearly_files_for_currency == []:
                               # print(f'{len(all_yearly_files_for_currency)} dataframes in full list')
                                currencydf = pandas.concat(all_yearly_files_for_currency)
                                currencydf = currencydf.loc[:, ~currencydf.columns.str.contains('^Unnamed')]
                                #currencydf.set_index('time', inplace=True)
                                currencydf.drop_duplicates(inplace=True)
                                currencydf.sort_index(inplace=True)
                                currencydf.to_csv(full_name)
                                print(f'finished full merge for {full_name}')
                        else:
                            print(f'no yearly data available to merge in {currency_path}')
                        print(f'merge for {c} in {t} res finsihed')
            else:
                print(f'no data present in {path}')
        else:
            print(f'{path} does not exist yet, get data first')
    print('merge completed')
    return True