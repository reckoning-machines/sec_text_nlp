from sec_text_nlp import *

wiki = wiki_sp500()

list_of_tickers = list(pd.read_csv('implementation_ticker_list.csv')['Symbol'])
print('Running for {0} tickers...'.format(len(list_of_tickers)))
list_of_tickers = ['AXP']
for ticker in list_of_tickers:
    print(ticker)
    
    print('noun phrases')
    stn = SECTextNLP(ticker)
    if hasattr(stn, 'df_mdna'):
        df = stn.textblob_nouns(stn.df_mdna)
        df = stn.NLTK_sentiment(df)
        if df is not None:
            stn.save_to_csv(df,'noun_phrases')
    else:
        print('Missing MDNA for '+ticker)

    df = stn.get_list_sentiment(stn.df_text,GLOBAL_SEARCH_LIST,'global_macro')
    if df is not None:
        stn.save_to_csv(df,'global_macro')
    
    print('list segments')
    list_segments = stn.get_noun_phrases_around_topic(BUSINESS_SEGMENT_LIST)
    if list_segments is not None:
        df = stn.get_list_sentiment(stn.df_text,list_segments,'segments')
        if df is not None:
            stn.save_to_csv(df,'segments_sentiment')
    
    print('list_people')
    list_people = wiki.get_people(ticker)
    df = stn.get_list_sentiment(stn.df_text,list_people,'people')
    if df is not None:
        stn.save_to_csv(df,'people_sentiment')
    
    print('list_products')
    list_products = stn.get_noun_phrases_around_topic(PRODUCTS_LIST)
    if list_products is not None:
        df = stn.get_list_sentiment(stn.df_text,list_products,'products')
        if df is not None:
            stn.save_to_csv(df,'products_sentiment')
    
    print('list_trademarks')
    list_trademarks = stn.get_words_with_trademark(stn.df_text) # this needs to be better formatted 
    if list_trademarks is not None:
        df = stn.get_list_sentiment(stn.df_text,list_trademarks,'trademarks')
        if df is not None:
            stn.save_to_csv(df,'trademarks_sentiment')

