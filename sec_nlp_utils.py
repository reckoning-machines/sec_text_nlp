import nltk
#nltk.download('punkt')
#nltk.download('vader_lexicon')
#nltk.download('stopwords')
#from transformers import pipeline
#nlp = pipeline('sentiment-analysis')

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize

#replace with your own list of words ... like covid or delay or cancel (it lowercases automatically)
FIND_WORDS = ['covid',
              'guidance',
              'outlook']

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
