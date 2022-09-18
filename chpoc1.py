import pandas as pd
from yahoofinancials import YahooFinancials
from datetime import date
import numpy as np
import datetime
import logging
from sklearn.linear_model import LinearRegression
import numpy as np 
import pyodbc
#import azure.functions as func
from threading import Thread
import threading


lock = threading.Lock()

def start():
    #threads = []
    tickerData = getTickerData()
    truncate_table('dbo.DashBoardData')
    grandTotalRows = Update(tickerData)
    # if isinstance(tickerData, pd.DataFrame):
    #     if len(tickerData) >= 4:
    #         truncate_table('dbo.DashBoardData')

    #         num_threads = 4
    #         index_to_split = len(tickerData) // num_threads
    #         remainder = len(tickerData) % num_threads
    #         start = 0
    #         end = index_to_split
    #         for i in range(num_threads):
    #             if i == 4:
    #                 df = tickerData.iloc[start:end+remainder, :]
    #             else:    
    #                 df = tickerData.iloc[start:end, :]

    #             t = Thread(target=Update, args=(df,)) # args[] contains function arguments
    #             t.start()                             # thread is now running!
    #             threads.append(t)                     # hold onto it for it later
    #             start += index_to_split
    #             end += index_to_split

    #         for t in threads:
    #             t.join()                              # waits for each t to finish            
    #     else:
    #         Update(tickerData)
    # else:
    #     logging.info("Unable to retrieve data from Stocklist table.")
    logging.info('Update completed. Grand total rows: ' + str(grandTotalRows))
    print('Update completed. Grand total rows: ' + str(grandTotalRows))

def get_db_connection():
    server = 'p5dzlm247r.database.windows.net' 
    database = 'chambers' 
    username = 'ajayarora@p5dzlm247r' 
    password = 'Aj@yAr0ra' 

    try:
        #cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        cnxn = pyodbc.connect('DRIVER={FreeTDS};SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    except:
        logging.error ('Cannot connect to Azure SQL Server')
    return cnxn   


def getModel(ticker_df):
   
    x = (ticker_df['date']).to_numpy(copy=True).reshape(-1,1)

    y = (ticker_df["adjclose"]).to_numpy(copy=True).reshape(-1,1)

    model = LinearRegression(fit_intercept=True).fit((x), np.log(y)) 

    # Print the Intercept: b
    #print('intercept 1:', (model.intercept_))

    # Print the Slope: m (growth factor)
    #print('slope 1:', (model.coef_[0]))

    return model



def getTickerData():
    cnxn = get_db_connection()
    if cnxn is None:
        return "Error connecting to DB."
    SQL_data = pd.read_sql_query("Select * from StockList", cnxn)
    return SQL_data

def truncate_table(table_ref):
    try:
        dbc = get_db_connection()
        with dbc.cursor() as cursor:
            cursor.execute(f'TRUNCATE TABLE {table_ref}')
            cursor.commit()
    except Exception as err:
        dbc.rollback()
        logging.error(err)
        raise err

def Update(SQL_data):
    format = '%Y-%m-%d'
    grandTotalRows = 0
    #lock.acquire()
    cnxn = get_db_connection()
    if cnxn is None:
        #lock.release()
        return "Error connecting to DB."

    for index, row in SQL_data.iterrows():
        try: 

            ticker = row["Ticker"]

            startDate = row['Chart Start']
            sdstr = (startDate)  #retrieve Chart Start date
            sdstr = sdstr.strftime(format)
            today = date.today()
            ed = today.strftime(format)
            
            yf = YahooFinancials(ticker)
            stockData = yf.get_historical_price_data(start_date=sdstr, end_date=ed, time_interval='daily')        

            ticker_df = pd.DataFrame(stockData[ticker]['prices'])

            if ticker_df.shape[0] <= 0:
                continue

            max_day_seconds = (np.max(ticker_df["date"]))
            #print(max_day_seconds)
            totalRows = len(ticker_df.index)
            #print(totalRows)
            previous_day_seconds = ticker_df.loc[totalRows-2,"date"]    


            df1 = pd.DataFrame(ticker_df[ticker_df["date"] == max_day_seconds])
            df2 = pd.DataFrame(ticker_df[ticker_df["date"] == previous_day_seconds])

            model = getModel(ticker_df)


            #predicted_values = (np.exp(model.predict(xx)))
            b = np.exp(model.intercept_)
            m = np.exp(model.coef_[0])

            ChgPrDay = float(df1["close"].values - df2["close"].values)
            FAGR = float((m[0] **(365.25*86400))-1) #** raise to power
            CurrentPrice = float(df1["adjclose"].values)


            ch1 = float(b * row["CoS1"] * np.exp((np.log(m) * max_day_seconds)))
            ch2 = float(b * row["CoS2"] * np.exp((np.log(m) * max_day_seconds)))
            ch3 = float(b * row["CoS3"] * np.exp((np.log(m) * max_day_seconds)))
            ch4 = float(b * row["CoS4"] * np.exp((np.log(m) * max_day_seconds)))
            ch5 = float(b * row["CoS5"] * np.exp((np.log(m) * max_day_seconds)))
            ch6 = float(b * row["CoS6"] * np.exp((np.log(m) * max_day_seconds)))
            ch7 = float(b * row["CoS7"] * np.exp((np.log(m) * max_day_seconds)))

            CPCh1 = float((CurrentPrice / ch1) - 1)
            CPCh2 = float((CurrentPrice / ch2) - 1)
            CPCh3 = float((CurrentPrice / ch3) - 1)
            CPCh4 = float((CurrentPrice / ch4) - 1)
            CPCh5 = float((CurrentPrice / ch5) - 1)
            CPCh6 = float((CurrentPrice / ch6) - 1)
            CPCh7 = float((CurrentPrice / ch7) - 1)

            Portfolio = row["Portfolio"]
            cursor = cnxn.cursor()
            cursor.execute("INSERT INTO dbo.DashBoardData (Ticker,Portfolio,FAGR, CurrentPrice, \
                            ChgPrDay, Ch1, Ch2, Ch3, Ch4, Ch5, Ch6, Ch7, \
                            CPCh1, CPCh2, CPCh3, CPCh4, CPCh5, CPCh6, CPCh7) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                            ticker, Portfolio, FAGR, CurrentPrice, ChgPrDay, ch1, ch2, ch3, ch4, ch5, ch6, ch7,
                            CPCh1, CPCh2, CPCh3, CPCh4, CPCh5, CPCh6, CPCh7 )
            cnxn.commit()     

            cursor.execute("INSERT INTO dbo.DashBoardData_Archive (Ticker,Portfolio,FAGR, CurrentPrice, \
                            ChgPrDay, Ch1, Ch2, Ch3, Ch4, Ch5, Ch6, Ch7, \
                            CPCh1, CPCh2, CPCh3, CPCh4, CPCh5, CPCh6, CPCh7) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                            ticker, Portfolio, FAGR, CurrentPrice, ChgPrDay, ch1, ch2, ch3, ch4, ch5, ch6, ch7,
                            CPCh1, CPCh2, CPCh3, CPCh4, CPCh5, CPCh6, CPCh7 )
            cnxn.commit()     
            grandTotalRows = grandTotalRows + totalRows
            #logging.info("Updated ticker . . . ( " + str(index) + " of " + str(len(SQL_data)) + ") " + str(ticker))
            print("Updated ticker . . . (" + str(index) + " of " + str(len(SQL_data)) + ") " + str(ticker) + " Rows: " + str(totalRows))
        except Exception as e:
            #logging.error("Ticker: " + str(ticker) + " " + str(e))
            print("Ticker: " + str(ticker) + " " + str(e))
            continue                
    if cnxn is not None:
        cursor.close()
    #lock.release()
    return grandTotalRows

if __name__ == "__main__":
    start()