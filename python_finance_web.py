# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 11:15:51 2017

@author: guof

https://www.economy.com/freelunch


"""

import quandl

import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web

style.use('ggplot')
start = dt.datetime(2000,1,1)
end = dt.datetime(2016,12,31)

#df = web.DataReader('TSLA', 'google', start, end)

df = quandl.get("WIKI/TSLA", authtoken = '4MuxRbJcWrNTt19L1rF-')

print(df.head())
#Sdf.plot()

#open = df.Open.resample('W-MON', how='last')
#open = df[Adj. Open].resample('W-MON', how='last')
open_wk = df['Adj. Open'].resample('W-MON').last()
close_wk = df['Adj. Close'].resample('W-FRI').last().resample('W-MON').last()
high_wk = df['Adj. High'].resample('W-MON').max()
low_wk = df['Adj. Low'].resample('W-MON').min()
vol_wk = df['Adj. Volume'].resample('W-MON').sum()

weekly = pd.concat([open_wk, close_wk, high_wk, low_wk, vol_wk],  axis=1)

print(weekly.head())

#df['Adj Close'].plot()