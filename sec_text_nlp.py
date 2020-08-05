

from sec_nlp_utils import *

class SECTextNLP(object):
    def __init__(self, ticker):
        self.ticker = ticker
        self.get_files_list()
        self.get_full_text()
        self.get_mdna_text()
        self.get_riskfactors_text()

    def get_files_list(self):
        self.df_file_index = pd.read_csv('file_index.csv', error_bad_lines=False) #error handle
        self.df_file_index = self.df_file_index.loc[self.df_file_index['ticker']==self.ticker]
        self.df_file_index.columns=self.df_file_index.columns.str.replace('\.','')
        self.df_file_index.columns=self.df_file_index.columns.str.replace('\d','')
        self.df_file_index['local_file_name'] = self.df_file_index['href'].str.replace("https://www.sec.gov",str(LOCAL_PATH))
        self.df_file_index = self.df_file_index.loc[:,~self.df_file_index.columns.duplicated()]
        self.list_href = list(self.df_file_index['href'])
    
    def NLTK_sentiment(self,df): #score each sentence
        sid = SentimentIntensityAnalyzer()
        for i,row in df.iterrows():
            sentence = row['sentence_text']
            sentence = filter_stopwords(sentence)
            ss = sid.polarity_scores(sentence)
            for ss_key in ss.keys():
                df.at[i,ss_key] = ss[ss_key]
        return df

    def pattern_sentiment(self,df): #score each sentence
        for i,row in df.iterrows():
            sentence = row['sentence_text']
            sentence = filter_stopwords(sentence)
            i_sentiment,i_subjectivity = sentiment(sentence)
            df.at[i,'pattern_sentiment'] = i_sentiment
            df.at[i,'pattern_subjectivity'] = i_subjectivity
        return df

    def match_keywords(self,df,keyword_list,keyword_list_name,return_list = 'wide'): #return match list of keyword, sentence id, one to many
        # allow for ability to explode into long form on a switch in the parameters
        # return wide or return long

        for i,row in df.iterrows():
            sentence = row['sentence_text']
            sentence = filter_stopwords(sentence)
            list_found = check_if_list_found_in_text(sentence,keyword_list)
            if len(list_found) >= 1:
                num_found = len(list_found)
                try:
                    df.at[i,keyword_list_name] = str(list_found)
                    df.at[i,keyword_list_name+'_number'] = num_found
                except:
                    print("Error: "+str(list_found)+":"+str(i))
        return df

    def get_mdna_text(self):
        list_mdna = []
        for href in self.list_href:
            file_name = href.replace("https://www.sec.gov",str(LOCAL_PATH))
            file_name_list = file_name.split('/')
            file_path = ''
            for s in file_name_list[:-1]:
                if len(s)>0:
                    file_path = file_path+"/"+s
            file_path = file_path + "/"
            only_files = [f for f in listdir(file_path) if isfile(join(file_path, f))]
            for f in only_files:
                if 'mdna' in f:
                    df_mdna = pd.read_csv(file_path + f)
                    df_mdna['file']=f
                    df_mdna['href']=href
                    list_mdna.append(df_mdna)
        if len(list_mdna) > 0:
            self.df_mdna = pd.concat(list_mdna)
        return
    
    def get_riskfactors_text(self):    
        list_rf = []
        for href in self.list_href:
            file_name = href.replace("https://www.sec.gov",str(LOCAL_PATH))
            file_name_list = file_name.split('/')
            file_path = ''
            for s in file_name_list[:-1]:
                if len(s)>0:
                    file_path = file_path+"/"+s
            file_path = file_path + "/"
            only_files = [f for f in listdir(file_path) if isfile(join(file_path, f))]
            for f in only_files:
                if 'riskfactors' in f:
                    df_rf = pd.read_csv(file_path + f)
                    df_rf['file']=f
                    df_rf['href']=href
                    list_rf.append(df_rf)
        if len(list_rf) > 0:
            self.df_riskfactors = pd.concat(list_rf)
        return
    
    def get_full_text(self):
        list_text = []
        for href in self.list_href:
            file_name = href.replace("https://www.sec.gov",str(LOCAL_PATH))
            file_name_list = file_name.split('/')
            file_path = ''
            for s in file_name_list[:-1]:
                if len(s)>0:
                    file_path = file_path+"/"+s
            file_path = file_path + "/"
            only_files = [f for f in listdir(file_path) if isfile(join(file_path, f))]
            for f in only_files:
                if 'sentences' in f:
                    df = pd.read_csv(file_path + f)
                    df['file']=f
                    df['href']=href
                    list_text.append(df)
        self.df_text = pd.concat(list_text)
        return 

    def get_words_with_trademark(self,df):
        #TO DO work on bigrams

        from nltk.tokenize import word_tokenize
        list_output = []
        text = ' '.join(list(df['sentence_text']))
        #blob = TextBlob(text) # too slow
        tokenized = list(set(word_tokenize(text)))
        #tokenized = list(set(list(blob.noun_phrases))) # too slow
        #print(len(tokenized))
        for w in tokenized:
            res = re.search(u'(\N{COPYRIGHT SIGN}|\N{TRADE MARK SIGN}|\N{REGISTERED SIGN})', w)
            if res:
                list_output.append(w)
        return list_output
        
    def get_noun_phrases_around_topic(self,topic_list,nrows = 10,depth = 5):
        for item in topic_list:
            i = np.flatnonzero(self.df_text['sentence_text'].str.contains(item))
            if len(i) != 0:
                break

        if len(i) == 0:
            return None

        list_nouns = []
        for item in i[:depth]:
            text = ' '.join(list(self.df_text[item:item+nrows]['sentence_text']))
            blob = TextBlob(text)
            list_nouns = list(set(list_nouns) | set(list(blob.noun_phrases)))

        list_nouns = [x for x in list_nouns if not any(c.isdigit() for c in x)]
        list_nouns = [re.sub(r"[-()\"\$\%#/@;:<>{}`+=~|.!?,]", "", x) for x in list_nouns]
        list_nouns = [x.strip(' ') for x in list_nouns]

        #pass thru common phrase filters
        list_filter = list(pd.read_csv(FNCL_TERMS_CSV,header=None)[0])
        list_main = np.setdiff1d(list_nouns,list_filter)
        list_filter = list(pd.read_csv(CORP_TERMS_CSV,header=None)[0])
        list_main = np.setdiff1d(list_main,list_filter)
        list_filter = list(pd.read_csv(LEGAL_TERMS_CSV,header=None)[0])
        list_main = np.setdiff1d(list_main,list_filter)
        list_filter = TERMS_FILTER_LIST
        list_main = np.setdiff1d(list_main,list_filter)

        #clean up the list a bit
        list_main = list(list_main)

        while("" in list_main) : 
            list_main.remove("")

        for item in TERMS_FUZZY_FILTER:
            list_main[:] = [x for x in list_main if item not in x]
        
        return list_main

