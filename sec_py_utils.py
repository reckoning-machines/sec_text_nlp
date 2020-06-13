
import time
import nltk
nltk.download('punkt')
nltk.download('vader_lexicon')
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize
import pandas as pd
pd.options.mode.chained_assignment = None
import os
import os.path
from os import path

import numpy as np
from pandarallel import pandarallel


LOGFILE = 'sec_nlp_beta.log'
f = open(LOGFILE, "w")
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
f.write(current_time+": process started")
f.close()

def filter_stopwords(sent):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(sent)
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    filtered_sentence = []
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    return ' '.join(filtered_sentence)

def filter_stopwords(sent):
  stop_words = set(stopwords.words('english'))
  word_tokens = word_tokenize(sent)
  filtered_sentence = [w for w in word_tokens if not w in stop_words]
  filtered_sentence = []
  for w in word_tokens:
      if w not in stop_words:
          filtered_sentence.append(w)
  return ' '.join(filtered_sentence)

def df_from_text(text):
  sentence_list = tokenize.sent_tokenize(text)
  sentence_list
  sid = SentimentIntensityAnalyzer()
  list_df = []
  for sentence in sentence_list:
      #
      # YOUR CODE HERE TO SCORE EACH sentence
      #
      sentence = filter_stopwords(sentence)
      ss = sid.polarity_scores(sentence)
      df = pd.DataFrame.from_dict(ss,orient = "index").T
      df['text'] = sentence
      list_df.append(df)
      return pd.concat(list_df)

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
    return pd.Series([row['ticker'],row['section'],row['type'],row['period_date'],neu,pos,neg,compound,text,num_rows])
