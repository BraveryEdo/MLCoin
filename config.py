csv_path = 'csv_data'
#2016 works for BTC, 2017 for ETC
start_year = 2020

# I am only interested in a few currencies that I want to trade, so let's add them here:
MY_CURRENCIES = ['BTC-USD','ETH-USD','LTC-USD','ALGO-USD']
#MY_CURRENCIES = ['BTC-USD', 'ETH-USD', 'LTC-USD']
START_YEAR_BY_CURRENCY = {'BTC-USD':2016, 'ETH-USD':2017}
TIME_SCALE = '5min'

#time_lens = {'1min':60, '5min':300, '15min':900, '1hr':3600, '6hr':21600, '1day':86400}
time_lens = {'1hr':3600, '6hr':21600, '1day':86400}

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