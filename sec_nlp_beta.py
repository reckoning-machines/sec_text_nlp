import os

#os.system("git clone https://gist.github.com/dc7e60aa487430ea704a8cb3f2c5d6a6.git /tmp/colab_util_repo")
#os.system("mv /tmp/colab_util_repo/colab_util.py colab_util.py")
#os.system("rm -r /tmp/colab_util_repo")
#os.system("mkdir sec_data_folder")
import pandas as pd
from sec_nlp_utils import *
import os.path
from os import path

import numpy as np
from pandarallel import pandarallel

import time

pandarallel.initialize()

LOGFILE = 'sec_nlp_beta.log'
f = open(LOGFILE, "w")
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
f.write(current_time+": process started")
f.close()

def py_write_log(str_text):
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(str_text)
    f = open(LOGFILE, "a")
    f.write(current_time+": "+str_text)
    f.close()
    return

def func_sentiment(row):
    df = df_from_text(row['text']) #neg neu pos compound text keywords_found
    neu = df.iloc[0]['neu']
    pos = df.iloc[0]['pos']
    neg = df.iloc[0]['neg']
    num_rows = 1
    compound = df.iloc[0]['compound']
    text = df.iloc[0]['text']
    keywords_found = df.iloc[0]['keywords_found']
    return pd.Series([row['ticker'],row['section'],row['type'],row['period_date'],neu,pos,neg,compound,keywords_found,text,num_rows])

df_tickers = pd.read_csv('implementation_ticker_list.csv')
#df_tickers = df_tickers[df_tickers['Symbol']=='CAH']
#read and process data into a single dataset.  add ticker along the way.
import time

master_list_df = []
list_tickers = df_tickers['Symbol']
#list_tickers = ['MMM']

for ticker in list_tickers:
    py_write_log("working on..."+ticker)
    tic = time.perf_counter()

    if path.exists("sec_data_folder/"+ticker+".csv"):

        df_text = pd.read_csv("sec_data_folder/"+ticker+".csv")
        if len(df_text) > 0:

            df_text['ticker'] = ticker

            df_discussion = df_text[df_text['section']=='discussion']

            df_out = df_discussion.parallel_apply(func_sentiment, axis=1)
            df_out.columns = ['ticker','section','type','period_date','neu','neg','pos','compound','keywords_found','text','num_rows']
            #df_out.to_csv("test.csv")
            if len(df_out) > 0:

                df_out = df_out.groupby(['ticker','period_date','type']).sum().reset_index()
                df_out['neg'] = df_out['neg']/df_out['num_rows']
                df_out['neu'] = df_out['neu']/df_out['num_rows']
                df_out['pos'] = df_out['pos']/df_out['num_rows']
                df_out['compound'] = df_out['compound']/df_out['num_rows']

                df_error = df_out[df_out['compound'] == 0]
                if not df_error.empty:
                    py_write_log("zero values..."+ticker)
                    df_error.to_csv('sec_nlp_errors.csv',mode = 'a')

                df_out.drop(['keywords_found'],axis = 1)
                df_out['compound_baseline'] = df_out['compound'] / df_out['compound'].mean()
                df_out['neg_baseline'] = df_out['neg'] / df_out['neg'].mean()
                df_out['pos_baseline'] = df_out['pos'] / df_out['pos'].mean()
                df_out['compound_bdiff'] = df_out['compound_baseline'].diff()
                df_out['neg_bdiff'] = df_out['neg_baseline'].diff()
                df_out['pos_bdiff'] = df_out['pos_baseline'].diff()
                df_out['compound_zscore'] = (df_out['compound'] - df_out['compound'].mean())/df_out['compound'].std(ddof=0)

                #always cache
                str_score_file = "sec_data_folder/"+ticker+"_score.csv"
                df_out.to_csv(str_score_file)

                master_list_df.append(df_out)
            else:
                py_write_log("missing..."+ticker)
        else:
            py_write_log(ticker+" has no data.")
    toc = time.perf_counter()
    py_write_log(f"Text processed in {toc - tic:0.4f} seconds")

if master_list_df:
    df_data = pd.concat(master_list_df)
    df_data.to_csv('df_data.csv')

#print("done!")
df = df_data[['period_date','ticker','compound_baseline']]
df['quarter_end'] = pd.to_datetime(df['period_date'])
df['quarter_end'] = df['quarter_end'].dt.to_period('q').dt.end_time
df['quarter_end'] = df['quarter_end'].dt.date
import numpy as np
df_data_pivot = pd.pivot_table(df, values='compound_baseline', index=['quarter_end'],
                    columns=['ticker'], aggfunc=np.sum, fill_value=0).reset_index()

df_data_pivot.to_csv("df_data_pivot.csv")

#figure out how to push to google sheets
#drive_handler.upload('df_data_pivot.csv', parent_path='sec_data_folder',overwrite=False)

print('done!')
