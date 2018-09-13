# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 20:58:51 2017

@author: guof
"""
import tushare as ts


df = ts.get_day_all("2018-09-13")
print(df.head)
