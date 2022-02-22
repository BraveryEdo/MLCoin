import pandas
import time
import datetime
import os


time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}


def addReadableTime():

    for t in time_lens:
        path = f'csv_data_{t}_res'
        if os.path.isdir(path):
            if not os.listdir(path) == []:
                for c in os.listdir(path):
                    currency_path = f'{path}\\{c}'
                    if os.path.isdir(currency_path):
                        if not os.listdir(currency_path) == []:
                            for y in os.listdir(currency_path):
                                y_path = f'{currency_path}\\{y}'
                                if not os.listdir(y_path) == []:
                                    for f in os.listdir(y_path):
                                        f_path = f'{y_path}\\{f}'
                                        if and(os.path.exists(f), os.path.is_file(f_path)):
                                            df = pandas.read_csv(f_path)
                                            og_time = df['time']

                                         