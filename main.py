import argparse
import requests
import os
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import struct
from lzma import LZMADecompressor, FORMAT_AUTO
from concurrent.futures import ThreadPoolExecutor, as_completed


DUKASCOPY_URL = "https://www.dukascopy.com/datafeed/{}/{}/{}/{}/{}h_ticks.bi5"
MAX_ATTEMPS = 5

def get_url_request(symbol, date_time, hour):
    return DUKASCOPY_URL.format(symbol, 
        date_time.year, 
        "{:02d}".format(date_time.month - 1),
        "{:02d}".format(date_time.day),
        "{:02d}".format(hour))


def download_file(url, location_dir, date_ts):
    os.makedirs(location_dir, exist_ok=True)
    print("Downloading file: {}".format(url))
    res = requests.get(url, stream=True)
    if res.status_code == 200:
        rawdata = res.content
        decomp = LZMADecompressor(FORMAT_AUTO, None, None)
        decompresseddata = decomp.decompress(rawdata)

        data = []
        for i in range(0, int(len(decompresseddata) / 20)):
            data.append(struct.unpack('!IIIff', decompresseddata[i * 20: (i + 1) * 20]))
        
        if len(data) == 0:
            return
        df = pd.DataFrame(data)
        df.columns = ['UTC', 'AskPrice', 'BidPrice', 'AskVolume', 'BidVolume']
        df.AskPrice = df.AskPrice / 100000
        df.BidPrice = df.BidPrice / 100000
        filename = url.split('/')[-1]
        hours = int(filename[0:2])   # extract hours from file name
        df.UTC = pd.TimedeltaIndex(df.UTC, 'ms')
        df.UTC = df.UTC + np.timedelta64(hours, 'h')
        df.UTC = df.UTC.astype(str)
        df.UTC = df.UTC.replace(regex=['0 days'], value=[str(date_ts)])
        df.UTC = df.UTC.str[:-3]
        df.to_parquet(location_dir + "/" + url.split('/')[-1] + ".parquet")
    else:
        print(res.content)


def download_data(folder, symbol , start_date, end_date):
    while start_date <= end_date:
        date_time_string = start_date.strftime("%Y-%m-%d")
        dir = folder + "/" + symbol +  "/{:02d}".format(start_date.year) + "/{:02d}".format(start_date.month) + "/" + date_time_string
        for hour in range(24):
            url = get_url_request(symbol=symbol, date_time=start_date, hour=hour)
            count = 0
            while count < MAX_ATTEMPS:
                count = count + 1
                try:
                    download_file(url, dir, date_ts=date_time_string)
                    break
                except:
                   pass
            if count == MAX_ATTEMPS:
                print("Failed to download file: {} after {} attemps".format(MAX_ATTEMPS, url))
        start_date += timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ticks Data Managment.')
    parser.add_argument('-d', '--directory', help='location to store the downloaded files', action='store', type=str, default='tickdata', required=False)
    parser.add_argument('-c', '--currency', help='Symbols: EURUSD, GBPUSD, EURGBP', action='store', type=str, default='', required=True)
    parser.add_argument('-s', '--start_date', help='Start date to download 2020-01-01', action='store', type=str, default='', required=True)
    parser.add_argument('-e', '--end_date', help='End date to download 2020-01-01', action='store', type=str, default='', required=False)
    parser.add_argument('-n', '--max_thread', help='Number thread dodwnload', action='store', type=int, default=4, required=False)
    args = parser.parse_args()

    symbol = args.currency
    dir = args.directory
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.datetime.now()
    
    if args.end_date != '':
        end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    number_day = (end_date - start_date).days
    days_range  = int(number_day / args.max_thread)
    
    start = ''
    end = ''

    with ThreadPoolExecutor(max_workers = args.max_thread) as executor:
        # Create threads
        while start_date <= end_date:
            if start_date + timedelta(days=days_range) <= end_date:
                start = start_date
                end = start_date + timedelta(days=days_range)
            else:
                start = start_date
                end = end_date

            print("Starting a thread for downloading range: ", start, start_date + timedelta(days=days_range))
            futures = {executor.submit(download_data, dir, symbol, start, end)}
            start_date = start_date + timedelta(days=days_range + 1)

        # as_completed() gives you the threads once finished
        for f in as_completed(futures):
            rs = f.result()