import pandas as pd

df_data = pd.read_csv('df_data.csv')

#print("done!")
df = df_data[['period_date','ticker','compound_baseline']]
df['quarter_end'] = pd.to_datetime(df['period_date'])
df['quarter_end'] = df.quarter_end.map(lambda x: x.strftime('%Y-%m-%d'))
df.loc[df.quarter_end == '2017-04-01', 'quarter_end'] = '2017-03-31'
df.loc[df.quarter_end == '2017-07-01', 'quarter_end'] = '2017-06-30'

#print(df[df['quarter_end']=='2017-04-01'])
df['quarter_end'] = pd.to_datetime(df['quarter_end'])
df['quarter_end'] = df['quarter_end'].dt.to_period('q').dt.end_time
df['quarter_end'] = df.quarter_end.map(lambda x: x.strftime('%Y-%m-%d'))
#df['quarter_end'] = df['quarter_end'].dt.date
#print(df[df['quarter_end']=='2017-04-01'])
print(df[df['ticker']=='BAC'])
import numpy as np
df_data_pivot = pd.pivot_table(df, values='compound_baseline', index=['quarter_end'],
                columns=['ticker'], aggfunc=np.sum, fill_value=0).reset_index()

df_data_pivot.to_csv("df_data_pivot.csv")
#print(df_data_pivot['AAPL'])
