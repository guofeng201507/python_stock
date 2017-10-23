# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 15:17:40 2017

@author: guof
"""

"""
https://pythonprogramming.net/current-yahoo-data-for-machine-learning/

"""


import time

from urllib2 import urlopen
import pandas as pd

import re

import os

#stock_list = ['LMT']
stock_list = ['LMT', 'BABA', 'TSLA', 'MULE', '0700.HK', '2318.HK', '2202.HK', '1357.HK', '1211.HK', '2120.HK', 'AU8U.SI', 'CWP.AX', 'IAG.AX', 'BEN.AX', 'A2M.AX']


def Check_Yahoo():

    for e in stock_list[0:]:
        try:

            link = "http://finance.yahoo.com/q/ks?s="+e+"+Key+Statistics"
            resp = urlopen(link).read()

            print e+"\tPage loaded"

            save = "forward/"+str(e)+".html"
            store = open(save,"w")
            store.write(str(resp))
            store.close()

        except Exception as e:
            print(str(e))
            #time.sleep(2)

def Forward(gather=[
                    'postMarketPrice',
                    'Price/Book',
                    'Profit Margin',
                    'Operating Margin',
                    #'Return on Equity',
                    'returnOnEquity',
                    'Forward P/E',
                    'PEG Ratio',
                    'Quarterly Earnings Growth',
                    'Revenue Growth',
                    'Current Ratio',
                    'Cash Flow',
                    'beta',
                    '52 Week High',
                    '52 Week Low',
                    '50-Day Moving Average',
                    '200-Day Moving Average',
                    'Forward Annual Dividend Yield',
                    'Trailing Annual Dividend Yield',
                    '5 Year Average Dividend Yield',
                    'Payout Ratio',
                    'Held by Insiders',
                    'Held by Institutions',
                    'Short Ratio'
                    #'key statistics for'
                    ]):

    df = pd.DataFrame(columns = ['Date',
                                 'Ticker',
                                 'Price',
                                 'Price/Book',
                                 'Profit Margin',
                                 'Operating Margin',
                                 'Return on Equity',
                                 'Forward P/E',
                                 'PEG Ratio',
                                 'Quarterly Earnings Growth',
                                 'Revenue Growth',
                                 'Current Ratio',
                                 'Cash Flow',
                                 'Beta',
                                 '52 Week High',
                                '52 Week Low',
                                '50-Day Moving Average',
                                '200-Day Moving Average',
                                'Forward Annual Dividend Yield',
                                'Trailing Annual Dividend Yield',
                                '5 Year Average Dividend Yield',
                                'Payout Ratio',
                                 'Held by Insiders',
                                 'Held by Institutions',
                                 'Short Ratio'
                                 #'Company Name'
                                 ])

    file_list = os.listdir("forward")

    for each_file in file_list:
        ticker = each_file.split(".html")[0]
        full_file_path = "forward/"+each_file
        source = open(full_file_path,"r").read()

        print full_file_path
        try:
            value_list = []

            for each_data in gather:
#                print each_data
                try:
                    regex = re.escape(each_data) + r'.*?(\d{1,8}\.\d{1,8}M?B?|N/A)%?'
                    value = re.search(regex, source)

                    #values = value.groups()
                    #print values
                    value = (value.group(1))
                    if "B" in value:
                        value = float(value.replace("B",''))*1000000000

                    elif "M" in value:
                        value = float(value.replace("M",''))*1000000

                    value_list.append(value)


                except Exception as e:
                    value = "N/A"
                    value_list.append(value)

            if value_list.count("N/A") > 15:
                print 'NA more than 15, data not valid'
                pass
            else:
                #print 'NA less than 15'
                df = df.append({'Date': time.strftime("%x"),
                                        'Ticker':ticker,
                                        'Price':value_list[0],
                                        'Price/Book':value_list[1],
                                        'Profit Margin':value_list[2],
                                        'Operating Margin':value_list[3],
                                        'Return on Equity':value_list[4],
                                         'Forward P/E':value_list[5],
                                         'PEG Ratio':value_list[6],
                                         'Quarterly Earnings Growth':value_list[7],
                                         'Revenue Growth':value_list[8],
                                         'Current Ratio':value_list[9],
                                         'Cash Flow':value_list[10],
                                         'Beta':value_list[11],
                                         '52 Week High':value_list[12],
                                        '52 Week Low': value_list[13],
                                        '50-Day Moving Average': value_list[14],
                                        '200-Day Moving Average': value_list[15],
                                        'Forward Annual Dividend Yield': value_list[16],
                                        'Trailing Annual Dividend Yield':value_list[17],
                                        '5 Year Average Dividend Yield': value_list[18],
                                        'Payout Ratio': value_list[19],
                                         'Held by Insiders':value_list[20],
                                         'Held by Institutions':value_list[21],
                                         'Short Ratio':value_list[22]
                                         #'Company Name':value_list[23]
                                 },
                                       ignore_index=True)
        except Exception as e:
            pass

    df.to_csv("forward_sample_WITH_NA.csv")


Check_Yahoo()
Forward()