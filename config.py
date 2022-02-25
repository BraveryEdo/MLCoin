sentiment_data_path = 'sentiment_data'
csv_path = 'csv_data'
start_year = 2016
MY_CURRENCIES = ['BTC-USD','ETH-USD','LTC-USD','ALGO-USD']
START_YEAR_BY_CURRENCY = {'BTC-USD':2016, 'ETH-USD':2017, 'LTC-USD':2017, 'ALGO-USD':2020}
time_lens = {'15min':900, '1hr':3600, '6hr':21600, '1day':86400}
#time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}
max_tries = 20
csv_zero_pad_for_sequence = 7

'''
csv data structure:
root:
root level: [csv_data_{RESOLUTION}_res]:
    currency folder [BTC-USD]:
        full summry file *if generated*: FULL_[BTC-USD]_{RESOLUTION}.csv
        year folder [2020]:
            data: [BTC-USD]_{YEAR}_{sequence}.csv
            yearly data summary file *if generated*: {YEAR}_[BTC-USD]_{RESOLUTION}.csv
'''