import nltk
from pattern.en import sentiment
#from transformers import pipeline
#NLP = pipeline("sentiment-analysis")

#from tokenizers import BertTokenizer
#tokenizer = BertTokenizer.from_pretrained('bert-base-uncased',do_lower_case = True)

#nltk.download('punkt')
#nltk.download('vader_lexicon')
#nltk.download('stopwords')
#from transformers import pipeline
#nlp = pipeline('sentiment-analysis')
import html5lib
from bs4 import BeautifulSoup
import requests, re
import os
import pandas as pd
import pathlib
from os import listdir
from os.path import isfile, join
import numpy as np
from textblob import TextBlob
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize

FILE_INDEX = 'file_index.csv'
SERVICES_CSV = 'wiki_sp500_services.csv'
PRODUCTS_CSV = 'wiki_sp500_products.csv'
PEOPLE_CSV = 'wiki_sp500_people.csv'
FNCL_TERMS_CSV = 'fncl_terms.csv'
CORP_TERMS_CSV = 'corp_terms.csv'
LEGAL_TERMS_CSV = 'legal_terms.csv'
LOCAL_PATH = pathlib.Path().absolute()

BUSINESS_SEGMENT_LIST = ['business segment results','business segment','reportable operating segments']
PRODUCTS_LIST = ['services include']

TERMS_FILTER_LIST = ['company \'s',
                'company \'s customers',
                'financial statements',
                'geographic basis',
                'geographic locations',
                'geographic region',
                'geographic segments',
                'management approach designates',
                'part ii',
                'segments consist',
                'company evaluates',
                'company manages']

GLOBAL_SEARCH_LIST = ['covid',
                    'recession',
                    'global',
                    'virus',
                    'coronavirus',
                    'china',
                    'economy',
                    'gdp']

TERMS_FUZZY_FILTER = ['countries','similar','unique','various','company','inc']
path = os.getcwd()
path = path + '/sec_scored_data/'
SCORED_DATA_PATH = path
path = ''

#replace with your own list of words ... like covid or delay or cancel (it lowercases automatically)
FIND_WORDS = ['covid',
              'guidance',
              'outlook']

class wiki_sp500(object):
    def __init__(self):
        self.df_services,self.df_products,self.df_people = self.get_wiki_sp500(force = False)
    
    def get_wiki_sp500(self,force):
        if force == True:
            df = get_wiki_list_sp500()
            df['SERVICES'] = df['WIKI_URL'].apply(services_extractor)
            df['KEY_PEOPLE'] = df['WIKI_URL'].apply(people_extractor)
            df['PRODUCTS'] = df['WIKI_URL'].apply(products_extractor)
            df = clean_people(df)
            df_services,df_people,df_products = wiki_wide_to_long(df)
            df_services.to_csv(SERVICES_CSV)
            df_products.to_csv(PRODUCTS_CSV)
            df_people.to_csv(PEOPLE_CSV)
            return df_services,df_products,df_people
        else:
            return pd.read_csv(SERVICES_CSV),pd.read_csv(PRODUCTS_CSV),pd.read_csv(PEOPLE_CSV)
    
    def get_people(self,ticker):
        df = self.df_people.loc[self.df_people['TICKER']==ticker]
        return list(df['PEOPLE_LIST'])

    def get_services(self,ticker):
        df = self.df_services.loc[self.df_services['TICKER']==ticker]
        return list(df['SERVICES'])

    def get_products(self,ticker):
        df = self.df_products.loc[self.df_products['TICKER']==ticker]
        return list(df['PRODUCT'])

def check_if_list_found_in_text(text, words=[], return_offset=False, lower_text=True):
    result = []
    text = (
        " "
        + text.replace("_", " ")
        .replace("-", " ")
        .replace(",", " ")
        .replace(";", " ")
        .replace('"', " ")
        .replace(":", " ")
        .replace(".", " ")
        + " "
    )
    if lower_text:
        text = text.lower()
    for word in words:
        word = (
            " "
            + word.replace("_", " ")
            .replace("-", " ")
            .replace(",", " ")
            .replace(";", " ")
            .replace('"', " ")
            .replace(":", " ")
            .replace(".", " ")
            + " "
        )
        if lower_text:
            word = word.lower()
        if word in text:
            if return_offset:
                offset = text.find(word)
                # offset = offset if not offset else offset-1
                result.append(offset)
            else:
                result.append(word.strip())
    return result

def filter_stopwords(sent):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(sent)
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    filtered_sentence = []
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    return ' '.join(filtered_sentence)

def sentiment_from_text(sentence):
#  dict_sentiment = {}
  sentence = filter_stopwords(sentence)
  list_found = check_if_list_found_in_text(sentence,FIND_WORDS)
  num_found = len(list_found)
  ###
  ss = sid.polarity_scores(sentence) #NLTK
  df = pd.DataFrame.from_dict(ss,orient = "index").T
  df['transformers_score'] = dict_transformers['score'] #tranformers
  df['transformers_label'] = dict_transformers['label']
  df['text'] = sentence
  df['keywords_found'] = num_found
  return pd.concat(dict_sentiment)

  ###
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

def two_list_union(lst1, lst2): 
    final_list = list(set(lst1) | set(lst2)) 
    return final_list 

def df_from_text(text):
  sentence_list = tokenize.sent_tokenize(text)
  sid = SentimentIntensityAnalyzer()
  list_df = []
  for sentence in sentence_list:
      sentence = filter_stopwords(sentence)
      list_found = check_if_list_found_in_text(sentence,FIND_WORDS)
      num_found = len(list_found)
# Allocate a pipeline for sentiment-analysis
#nlp = pipeline('sentiment-analysis')
#nlp('We are very happy to include pipeline into the transformers repository.')
#>>> {'label': 'POSITIVE', 'score': 0.99893874}
## which contains pos, neg, neu, and compound scores.

      ss = sid.polarity_scores(sentence)
      df = pd.DataFrame.from_dict(ss,orient = "index").T
      df['text'] = sentence
      df['keywords_found'] = num_found
      list_df.append(df)
      return pd.concat(list_df)

def get_wiki_list_sp500():
    ticker_list = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = ticker_list[0]
    df.drop(columns = ['SEC filings', 'Date first added', 'Founded'], inplace = True)
    df.CIK = df.CIK.astype(str)
    df['CIK'] = df['CIK'].str.zfill(10)
    df.rename(columns = {'Symbol':'TICKER', 'Security':'COMPANY', 'GICS Sector':'GICS_SECTOR',
                     'GICS Sub Industry':'GICS_INDUSTRY', 'Headquarters Location':'HQ'}, inplace = True)
    df['WIKI_URL'] = ''
    request = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = BeautifulSoup(request.content)
    main_table = soup.find(id='constituents')
    table = main_table.find('tbody').findAll('tr')
    table = table[1:]
    base_url = 'https://en.wikipedia.org'
    url_list = []
    for item in table:
        url = base_url + str(item.findAll('a')[1]['href'])
        url_list.append(url)
    df['WIKI_URL'] = url_list
    df.to_csv('wiki_list_sp500.csv')
    return df

def people_extractor(url):

    output = 'none' # return none if field doesn't exist
    vcard_list = pd.read_html(url)
    df = vcard_list[0]
    
    if len(df.columns) == 2:
        df.columns = ['columns', 'data']
        # most output tables have 2 elements
    else:
        df.columns = ['columns', 'data', 'trash']
        df.drop(columns = 'trash', inplace = True)
        # sometimes the table output has 3 elements
        
    df.set_index(df['columns'], inplace = True)
    df.drop(columns = 'columns', inplace = True)
    df = df.transpose()
    
    if 'Key people' in df.columns:
        output = df['Key people'][0]
    
    return output

def products_extractor(url):

    output = 'none' # return none if field doesn't exist
    vcard_list = pd.read_html(url)
    df = vcard_list[0]
    
    if len(df.columns) == 2:
        df.columns = ['columns', 'data']
        # most output tables have 2 elements
    else:
        df.columns = ['columns', 'data', 'trash']
        df.drop(columns = 'trash', inplace = True)
        # sometimes the table output has 3 elements
        
    df.set_index(df['columns'], inplace = True)
    df.drop(columns = 'columns', inplace = True)
    #print(df)
    df = df.transpose()
    
    if 'Products' in df.columns:
        output = df['Products'][0]
        import re
        output = (re.findall('[A-Z][^A-Z]*', output))
    return output

def services_extractor(url):

    output = 'none' # return none if field doesn't exist
    vcard_list = pd.read_html(url)
    df = vcard_list[0]
    
    if len(df.columns) == 2:
        df.columns = ['columns', 'data']
        # most output tables have 2 elements
    else:
        df.columns = ['columns', 'data', 'trash']
        df.drop(columns = 'trash', inplace = True)
        # sometimes the table output has 3 elements
        
    df.set_index(df['columns'], inplace = True)
    df.drop(columns = 'columns', inplace = True)
    df = df.transpose()
    
    if 'Services' in df.columns:
        list_output = []
        str_list = df['Services'][0]
        #print(str_list)
        result = [x.strip() for x in str_list.split(',')]
        for w in result:
            w_list = re.findall('[A-Z][^A-Z]*', w)
            w = ' '.join(w_list)
            list_output.append(w)
        output = ','.join([str(elem) for elem in list_output])
        return list_output
    return 

def clean_people(df):
    df['PEOPLE_LIST'] = ''
    bad_list = []

    for i in df.index:

        inp = df['KEY_PEOPLE'][i]
    
        if inp != 'none':
        
        # Remove title words found outside parens or brackets
            text = inp.replace('and', '').replace('CFO', '').replace('CEO', '').replace('&', '').replace('.', '').replace(',', '')\
                  .replace('President', '').replace('/', '').replace('Pres', '').replace('Chairman', '').replace('General', '')\
                  .replace('counsel', '').replace('Chair', '').replace('Managing', '').replace('Director', '')\
                  .replace('Chief', '').replace('Executive', '').replace('Officer', '').replace('Jr', '').replace('of', '')\
                  .replace('the', '').replace('Board', '').replace('Governed', '').replace('14-member', '').replace('Trustees', '')\
                  .replace('Sanford', '').replace('Cloud', '').replace('Lead', '').replace('Trustee', '').replace('chief', '')\
                  .replace('executive', '').replace('officer', '').replace('ficer', '').replace('vice', '').replace('president', '')\
                  .replace('financial', '').replace('legal', '').replace('legal', '').replace('Dr', '').replace('chairman', '')\
                  .replace('SO', '')
        
        # Regex to remove all words inside parenthesis and brackets, special characters, numbers, etc;
            text1 = re.sub("[\(\[].*?[\)\]]", " ", text)
            text2 = re.sub(r"(\w)([A-Z])", r"\1 \2", text1)
            text3 = re.sub(r'"[^)]*"', '', text2)
            text4 = re.sub(' [A-Z]* ', ' ', text3)
            text5 = re.sub('[A-Z]* ', ' ', text4)
            text6 = re.sub(r'[^a-zA-Z0-9\s]', '', text5)
            text7 = re.sub('[0-9]', '', text6)
            out = text7.strip()
            word_list = out.split()
            word_list = [ ' '.join(x) for x in zip(word_list[0::2], word_list[1::2]) ]
            df['PEOPLE_LIST'][i] = word_list
        
        # Attempt at identifying lists that cannot be easily merged first name + last name by find lists with odd number of elements;
            if len(df['PEOPLE_LIST'][i]) % 2 != 0:
                bad_list.append(i)
    return df

def wiki_wide_to_long(df):
        list_products = []
        for i,row in df.iterrows():
            ticker = (row['TICKER'])
            products = (row['PRODUCTS'])
            if products !="none":
                try:
                    df_products = pd.DataFrame(row['PRODUCTS'])
                    df_products['TICKER']=ticker
                    if len(df_products.columns) == 2:
                        df_products.columns = ['PRODUCT','TICKER']
                        list_products.append(df_products)
                except:
                    print(ticker + ' products dataframe error')
        list_services = []
        for i,row in df.iterrows():
            ticker = (row['TICKER'])
            services = (row['SERVICES'])
            if services is not None and services != "none":
                try:
                    df_services = pd.DataFrame(row['SERVICES'])
                    df_services['TICKER']=ticker
                    if len(df_services.columns) == 2:
                        df_services.columns = ['SERVICES','TICKER']
                        df_services['SERVICES'] = df_services['SERVICES'].str.replace('\[1\]','')
                    list_services.append(df_services)
                except:
                    print(ticker+' services dataframe error')
        list_people = []
        for i,row in df.iterrows():
            ticker = (row['TICKER'])
            people = (row['PEOPLE_LIST'])
            if people !="none":
                try:
                    df_people = pd.DataFrame(row['PEOPLE_LIST'])
                    df_people['TICKER']=ticker
                    if len(df_people.columns) == 2:
                        df_people.columns = ['PEOPLE_LIST','TICKER']
                        list_people.append(df_people)
                except:
                    print(ticker+' people dataframe error')
        return pd.concat(list_services),pd.concat(list_people),pd.concat(list_products)

def split_keywords(df):
    list_df = []
    for i,row in df.iterrows():
        if not pd.isna(row['segments_number']):
            s = str(row['segments'])
            s = s.strip('[]')
            s = s.replace("'","")
            df_loop = pd.DataFrame(s.split(','))
            df_loop.columns=['keyword']
            df_loop['href'] = row['href']
            df_loop['sentence_text'] = row['sentence_text']
            df_loop['compound'] = row['compound']
            list_df.append(df_loop)
    df_out = pd.concat(list_df)
    return df_out    

def score_files(csv, force = False):
    df_ticker_list = pd.read_csv(csv)

    df_file_list = pd.read_csv(FILE_INDEX)

    for i, row in df_ticker_list.iterrows():

        ticker = row['Symbol']
        ticker = 'AXP'
        ticker = ticker.upper()
        file_path = SCORED_DATA_PATH + ticker
        file_name = file_path+"/"+ticker+'_keywords.csv'

        if force == False:
            if os.path.exists(file_name):
                print("File exists: "+file_name)
                continue

        df_filter = df_file_list.loc[df_file_list['ticker']==ticker]
        archive_dir = list(df_filter['href'])[0]
        archive_dir = archive_dir.replace("https://www.sec.gov",str(LOCAL_PATH))
        archive_dir_list = archive_dir.split('/')
        archive_path = ''
        for s in archive_dir_list[:-2]:
            if len(s)>0:
                archive_path = archive_path+"/"+s
        archive_path = archive_path + "/"
            
        print('Working for '+ticker+" at "+archive_path)

        if not os.path.exists(archive_path):
            print("Missing directory: "+archive_path)
            continue

        try:
            os.makedirs(file_path, exist_ok=True)
        except OSError:
            print ("Directory %s failed" % file_path)
        else:
            print ("Successfully created the directory %s " % file_path)

        try:
            print("SEC object")
            obj_sec = SECTextData(ticker)
        except:
            print ("failed creating SECTextData for "+ticker)
            continue

        if hasattr(obj_sec,"df_mdna") == False:
            print ("MDNA false: continue")
            continue

        if hasattr(obj_sec,"list_segments") == False:
            print ("List segments false: continue")
            continue

        print("NLP object")
        obj_nlp = NLPText(obj_sec.df_mdna,ticker)
        obj_nlp.match_keywords(obj_sec.list_segments,'segments')

        df = obj_nlp.df_tokenized
        df_out = split_keywords(df)

        print("Munge data")
        df_out = df_out.groupby(['href','keyword']).mean().reset_index()
        df_fi = pd.read_csv('file_index.csv')
        df_fi['filing_date'] = pd.to_datetime(df_fi['filing_date...4'])
        df_fi['period_date'] = pd.to_datetime(df_fi['period_date'])
        df_fi = df_fi[['ticker','href','period_date','filing_date']]
        df_fi = df_fi.groupby('href').first()
        df_merged = pd.merge(df_out,df_fi,how = 'inner',left_on = 'href',right_on = 'href')
        df_merged = df_merged.sort_values('filing_date',ascending = False)
    
        print("Write to file")
        df_merged.to_csv(file_name)
        break
