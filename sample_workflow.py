

from sec_text_nlp import *
#TICKER_LIST = 'test_ticker_list.csv'

#SECTION or TOPIC modeling
#revenue recognition
#join venture
#related party
#acquisitions
#commissions
#receivables
#dividends

stn = SECTextNLP("AAPL")
print(stn.df_file_index.columns)
print(stn.df_file_index)
print(stn.df_text)
#print(pd.merge(stn.df_text,stn.df_file_index,how = 'inner',left_on='href',right_on='href')[['ticker','filing_date','sentence_text']])
#print(pd.merge(stn.df_mdna,stn.df_file_index,how = 'inner',left_on='href',right_on='href')[['ticker','filing_date','sentence_text']])

#print('business segments')
#print(stn.get_noun_phrases_around_topic(BUSINESS_SEGMENT_LIST))
#print('products list')
#print(stn.get_noun_phrases_around_topic(PRODUCTS_LIST,nrows=2))
#print('trademarks')
#print(stn.get_words_with_trademark(stn.df_mdna))

#df = stn.match_keywords(stn.NLTK_sentiment(stn.df_mdna),GLOBAL_SEARCH_LIST,'MACRO')
#print(df)

#list_trademarks = stn.get_words_with_trademark(stn.df_mdna)
#df = stn.match_keywords(stn.NLTK_sentiment(stn.df_mdna),list_trademarks,'trademarks')
#print(df.dropna())

#list_trademarks = stn.get_words_with_trademark(stn.df_mdna)
#df = stn.match_keywords(stn.pattern_sentiment(stn.df_mdna),list_trademarks,'trademarks')
#print(df.dropna()[['sentence_text','pattern_sentiment','pattern_subjectivity','trademarks','trademarks_number']])

#to do return full custom section blocks
#example dividend policy, 5, df

