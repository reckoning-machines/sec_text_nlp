
#os.system("git clone https://gist.github.com/dc7e60aa487430ea704a8cb3f2c5d6a6.git /tmp/colab_util_repo")
#os.system("mv /tmp/colab_util_repo/colab_util.py colab_util.py")
#os.system("rm -r /tmp/colab_util_repo")
#os.system("mkdir sec_data_folder")
import os
import pandas as pd
import os.path
from os import path

from sec_py_utils import *

import numpy as np
from pandarallel import pandarallel

pandarallel.initialize()

df_tickers = pd.read_csv('test_ticker_list.csv')

master_list_df = []
list_tickers = df_tickers['Symbol']

for ticker in list_tickers:
    py_write_log("working on..."+ticker)
    tic = time.perf_counter()

    if path.exists("sec_data_folder/"+ticker+".csv"):

        df_text = pd.read_csv("sec_data_folder/"+ticker+".csv")
        if len(df_text) > 0:

            df_text['ticker'] = ticker

            df_discussion = df_text[df_text['section']=='discussion']

            df_out = df_discussion.parallel_apply(func_se ntiment, axis=1)
            df_out.columns = ['ticker','section','type','period_date','neu','neg','pos','compound','text','num_rows']

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

                df_out['compound_baseline'] = df_out['compound'] / df_out['compound'].mean()
                df_out['neg_baseline'] = df_out['neg'] / df_out['neg'].mean()
                df_out['pos_baseline'] = df_out['pos'] / df_out['pos'].mean()
                df_out['compound_bdiff'] = df_out['compound_baseline'].diff()
                df_out['neg_bdiff'] = df_out['neg_baseline'].diff()
                df_out['pos_bdiff'] = df_out['pos_baseline'].diff()
                df_out['compound_zscore'] = (df_out['compound'] - df_out['compound'].mean())/df_out['compound'].std(ddof=0)

                #to iterate is human, to cache is divine
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

df = df_data[['period_date','ticker','compound_baseline']]
df['quarter_end'] = pd.to_datetime(df['period_date'])
df['quarter_end'] = df.quarter_end.map(lambda x: x.strftime('%Y-%m-%d'))

#modify odd quarter ends
df.loc[df.quarter_end == '2017-04-01', 'quarter_end'] = '2017-03-31'
df.loc[df.quarter_end == '2017-07-01', 'quarter_end'] = '2017-06-30'
df.loc[df.quarter_end == '2018-04-01', 'quarter_end'] = '2018-03-31'
df.loc[df.quarter_end == '2018-07-01', 'quarter_end'] = '2018-06-30'

df['quarter_end'] = pd.to_datetime(df['quarter_end'])
df['quarter_end'] = df['quarter_end'].dt.to_period('q').dt.end_time #floor at end of quarter
df['quarter_end'] = df.quarter_end.map(lambda x: x.strftime('%Y-%m-%d')) #format

import numpy as np
df_data_pivot = pd.pivot_table(df, values='compound_baseline', index=['quarter_end'],
                columns=['ticker'], aggfunc=np.sum, fill_value=0).reset_index()

df_data_pivot.to_csv("df_data_pivot.csv")

print('done!')
