# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas_datareader.data as web
import pandas
import datetime    

from forex_python.converter import CurrencyRates

import sqlite3
from sqlite3 import Error

start = datetime.datetime(2013, 1, 1)
end = datetime.datetime(2017, 6, 8)

"""
df = web.DataReader("f", 'google', start, end)

Working Code: 
df= web.get_data_google("LMT")
df=web.get_quote_google("SGX:AU8U")
df=web.get_quote_google("ASX:IAG")
df=web.get_quote_google("HKG:0700")


"""


df=web.get_quote_google("SGX:AU8U")

"""

"""
dates =[]
for x in range(len(df)):
    newdate = str(df.index[x])
    newdate = newdate[0:10]
    dates.append(newdate)

df['dates'] = dates



conn = sqlite3.connect("C:\\Users\guof\AnacondaProjects\proj1\db\pythonsqlite_1.db")

pandas.DataFrame.to_sql(df, con=conn, flavor=None, name='Stocks', schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)

print df.head()
print df.tail()

rate = CurrencyRates()
sgd_r = rate.get_rates('SGD')

print sgd_r