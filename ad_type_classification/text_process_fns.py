import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer

nltk.download('stopwords')
nltk.download('punkt')
  
eng_stopwords = stopwords.words('english') 
lem = WordNetLemmatizer()
stem = PorterStemmer()
punct = set(x for x in string.punctuation)

def filter_stop_words(word_list):
    for word in word_list:
        if word in eng_stopwords:
            continue
        yield(word)

def stemmer(lst):
    return list(map(stem.stem, lst))

def filter_punct(x):
    return ''.join([w for w in x if w not in punct])


def process_creative_body(creative_body):
    for process in (filter_punct, word_tokenize, filter_stop_words, stemmer):
        creative_body = process(creative_body)
    return creative_body

