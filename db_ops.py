# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 11:53:13 2017

@author: guof
"""
import sqlite3
from sqlite3 import Error
import glob
import pandas as pd
import shutil
import datetime

from dateutil.relativedelta import relativedelta

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        
    except Error as e:
        print(e)
    finally:
        conn.close()
 
def load_all_csv(db_file, test):
             
    print(sqlite3.version)
    
    if test :
        path =r'C:\Users\guof\Desktop\Stock Research\hk-trading-data.20171019\Test_run' # use your path
    else:    
        path =r'C:\Users\guof\Desktop\Stock Research\hk-trading-data.20171019\stock data' # use your path

    allFiles = glob.glob(path + "/*.csv")
    
    try:
        conn = sqlite3.connect(db_file)
                
        for file_ in allFiles:
            print(path)
            df = pd.read_csv(file_, header = 0, encoding = 'GBK', parse_dates=[1])
            df.columns  = ["STOCK_CODE", "TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "PRE_CLOSE", "CHANGE", "VOLUME","VALUE"]
            print('This is the format of DF from CSV')
            print(df.head()) 
            #Write daily trade into RAW table
#Initial loading of creating table                  

            pd.DataFrame.to_sql(df, con=conn, flavor=None, name='TRADE_RAW_DAILY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)

    except Error as e:
        print(e)
    finally:
        conn.close()

def recur_daily_csv(db_file):
             
    path =r'C:\Users\guof\Desktop\Stock Research\daily' # use your path
    allFiles = glob.glob(path + "/*.csv")
    
    #path_bkup =r'C:\Users\guof\Desktop\Stock Research\daily_bkup' # use your path

    
    try:
        conn = sqlite3.connect(db_file)
                
        for file_ in allFiles:
#Encoding set as 'GBK' for chinese character header           
            #print(type(file_))
            df = pd.read_csv(file_, header = 0, encoding = 'GBK', parse_dates=[1])
            df.columns  = ["STOCK_CODE", "TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "PRE_CLOSE", "CHANGE", "VOLUME","VALUE"]
            print('This is the format of DF from CSV')
            print(df.head()) 
                        
#All the follow up loading.            
            pd.DataFrame.to_sql(df, con=conn, name='TRADE_RAW_DAILY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)            

#move daily file to backup folder
            file_bkup = file_.replace("daily", "daily_bkup")
            shutil.move(file_, file_bkup)
    except Error as e:
        print(e)
    finally:
        conn.close()

def compute_wkly_monthly(db_file):
    
        ma_list = [5, 10, 30] #Moving average period
        try:
            conn = sqlite3.connect(db_file)
            cur = conn.cursor()
            rows = cur.execute("SELECT distinct STOCK_CODE FROM TRADE_RAW_DAILY;").fetchall()
            
            cur.close()

#Process each stock code
            for row in rows:
                each_stock = "select * from TRADE_RAW_DAILY where STOCK_CODE = " + "\"" + row[0] + "\""
                stock_data = pd.read_sql_query(each_stock, conn)
#Data frame read from DB, although it is stored in datetime type, it has to be converted                                
                stock_data['TRADE_DATE'] = pd.to_datetime(stock_data['TRADE_DATE'])
                stock_data.set_index('TRADE_DATE', inplace=True)

#Start converting daily trade data into weekly
#W, M, Q, 5min, 12D                
#Compute weekly MA
                weekly_data = convert_period(stock_data, 'W')
                
                for ma in ma_list:
                    weekly_data['MA_' + str(ma)] = weekly_data['CLOSE'].rolling(window=ma, center=False).mean()

                print(weekly_data.head())
                pd.DataFrame.to_sql(weekly_data, con=conn, name='TRADE_WEEKLY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)       

#Compute monthly MA
                
                monthly_data = convert_period(stock_data, 'M')
                for ma in ma_list:
                    monthly_data['MA_' + str(ma)] = monthly_data['CLOSE'].rolling(window=ma, center=False).mean()
                print(monthly_data.head())
                pd.DataFrame.to_sql(monthly_data, con=conn, flavor=None, name='TRADE_MONTHLY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)       

            
        except Error as e:
            print(e)
        finally:
            conn.close()  

def convert_period(stock_data, period_type):
    
    period_stock_data = stock_data.resample(period_type).last()
    period_stock_data['CHANGE'] = stock_data['CHANGE'].resample(period_type).apply(lambda x: (x+1.0).prod() - 1.0)
    period_stock_data['OPEN'] = stock_data['OPEN'].resample(period_type).first()  
    period_stock_data['HIGH'] = stock_data['HIGH'].resample(period_type).max()        
    period_stock_data['LOW'] = stock_data['LOW'].resample(period_type).min()           
    period_stock_data['VOLUME'] = stock_data['VOLUME'].resample(period_type).sum()    
    period_stock_data['VALUE'] = stock_data['VALUE'].resample(period_type).sum()       
    
    period_stock_data = period_stock_data[period_stock_data['STOCK_CODE'].notnull()]
    
    period_stock_data.reset_index(inplace=True)
    
    return period_stock_data

def recur_wkly(db_file):
    
    ma_list = [5, 10, 30] #Moving average period
        
#Retrive last monday and Friday
    current_time = datetime.datetime.now()

    if current_time.isoweekday() == 5:
        friday = current_time.date()
    elif current_time.isoweekday() == 6:  
        friday = current_time.date() - datetime.timedelta(days=1)    
    # get friday, one week ago
    else:
        friday = (current_time.date() - datetime.timedelta(days=current_time.weekday())
        + datetime.timedelta(days=4, weeks=-1))
    
    print(friday)
        
    monday = friday - datetime.timedelta(days=4)    

    print(monday)   

    
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        rows = cur.execute("SELECT distinct STOCK_CODE FROM TRADE_RAW_DAILY;").fetchall()
        
        cur.close()

#Process each stock code
        for row in rows:
            each_stock = "select * from TRADE_RAW_DAILY where STOCK_CODE = " + "\"" + row[0] + "\""
            stock_data = pd.read_sql_query(each_stock, conn)
          
#Data frame read from DB, although it is stored in datetime type, it has to be converted                                
            stock_data['TRADE_DATE'] = pd.to_datetime(stock_data['TRADE_DATE'])
            
            stock_data = stock_data[(stock_data.TRADE_DATE >= monday) & (stock_data.TRADE_DATE <= friday)]
            
            stock_data.set_index('TRADE_DATE', inplace=True)
            
            #Need to sort this by trade_date, as the latest entries always at the bottom
            print(stock_data)

#Start converting daily trade data into weekly + MA            
            weekly_data = convert_period(stock_data, 'W')
           
            for ma in ma_list:
                #weekly_data['MA_' + str(ma)] = pd.rolling_mean(weekly_data['CLOSE'], ma)
                weekly_data['MA_' + str(ma)] = weekly_data['CLOSE'].rolling(window=ma, center=False).mean()

            print(weekly_data.head())
            pd.DataFrame.to_sql(weekly_data, con=conn, name='TRADE_WEEKLY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)       
       
    except Error as e:
        print(e)
    finally:
        conn.close()

def recur_monthly(db_file):
    
        ma_list = [5, 10, 30] #Moving average period
        
        #Retrive first day and last day of previous month
        today = datetime.date.today()
    
        first = today.replace(day=1)
        last_d_pre_m = first - datetime.timedelta(days=1)
        first_d_pre_m = first - relativedelta(months=1)
        print(first_d_pre_m)
        print(last_d_pre_m)
                
        try:
            conn = sqlite3.connect(db_file)
            cur = conn.cursor()
            rows = cur.execute("SELECT distinct STOCK_CODE FROM TRADE_RAW_DAILY;").fetchall()
            
            cur.close()

#Process each stock code
            for row in rows:
                each_stock = "select * from TRADE_RAW_DAILY where STOCK_CODE = " + "\"" + row[0] + "\""
                stock_data = pd.read_sql_query(each_stock, conn)
#Data frame read from DB, although it is stored in datetime type, it has to be converted                                
                stock_data['TRADE_DATE'] = pd.to_datetime(stock_data['TRADE_DATE'])
                
                stock_data = stock_data[(stock_data.TRADE_DATE >= first_d_pre_m) & (stock_data.TRADE_DATE <= last_d_pre_m)]
                
                stock_data.set_index('TRADE_DATE', inplace=True)
        
#Compute monthly MA
                
                monthly_data = convert_period(stock_data, 'M')
                for ma in ma_list:
                    monthly_data['MA_' + str(ma)] = monthly_data['CLOSE'].rolling(window=ma, center=False).mean()
                print(monthly_data.head())
                pd.DataFrame.to_sql(monthly_data, con=conn, flavor=None, name='TRADE_MONTHLY', schema=None, if_exists='append', index=False, index_label=False, chunksize=None, dtype=None)       
            
        except Error as e:
            print(e)
        finally:
            conn.close()





if __name__ == '__main__':
    
#    create_connection("C:\\Users\guof\AnacondaProjects\proj1\db\pythonsqlite_1.db")
#Test run    
    #load_all_csv("D:\\_STOCKDB\pythonsqlite_1.db", False)
#Real run     
    #load_all_csv("D:\\_STOCKDB\pythonsqlite_1.db", False)

    
    #compute_wkly_monthly("D:\\_STOCKDB\pythonsqlite_1.db")
    
#Operational functions
    #Load daily csv    
    #recur_daily_csv("D:\\_STOCKDB\pythonsqlite_1.db")
    
    recur_wkly("D:\\_STOCKDB\pythonsqlite_1.db")
    
    #recur_monthly("D:\\_STOCKDB\pythonsqlite_1.db")
    
"""    
Python can only create db file, thus the foler needs to be created first

http://www.sqlitetutorial.net/sqlite-python/create-tables/


"""

    
    
    
    