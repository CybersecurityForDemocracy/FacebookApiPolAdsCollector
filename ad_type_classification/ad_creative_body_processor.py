import itertools
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import unicodedata


class AdCreativeBodyProcessor:
    """Class to encapsulate ad creative body processing logic."""

    def __init__(self):
        nltk.download('stopwords')
        nltk.download('punkt')
        self.english_stopwords = stopwords.words('english')
        self._stemmer = PorterStemmer()

    def _filter_stop_words(self, word_list):
        for word in word_list:
            if word in self.english_stopwords:
                continue
            yield word

    def _get_stems(self, word_list):
        return list(map(self._stemmer.stem, word_list))

    def _filter_punct(self, input_text):
        """Removes punction from input. More information about Unicode categories
        https://www.unicode.org/reports/tr44/#General_Category_Values

        Args:
            input_text: str from which to remove punctuation.
        Returns:
            str of input_text with punctuation removed.
        """
        return ''.join(itertools.filterfalse(lambda x: unicodedata.category(x).startswith('P'),
                                             input_text))

    def process_creative_body(self, creative_body):
        """Removes punctuation, tokenizes into a list or words, removes stop words, and adds list of
        word stems to input.

        Args:
            creative_body: str ad creative text to be processed.
        Returns:
            list tokenized words with punctuation and stop words removed and word stems added.
        """
        return self._get_stems(self._filter_stop_words(word_tokenize(self._filter_punct(
                creative_body))))
