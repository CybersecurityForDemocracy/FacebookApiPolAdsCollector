import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer


class AdCreativeBodyProcessor:
    """Class to encapsulate ad creative body processing logic."""

    def __init__(self):
        nltk.download('stopwords')
        nltk.download('punkt')
        self.english_stopwords = stopwords.words('english')
        self.stemmer = PorterStemmer()

    def _filter_stop_words(self, word_list):
        for word in word_list:
            if word in self.english_stopwords:
                continue
            yield word

    def _stemmer(self, word_list):
        return list(map(self.stemmer.stem, word_list))

    def _filter_punct(self, word_list):
        return ''.join([w for w in word_list if w not in string.punctuation])

    def process_creative_body(self, creative_body):
        for process in (self._filter_punct, word_tokenize, self._filter_stop_words, self._stemmer):
            creative_body = process(creative_body)
        return creative_body
