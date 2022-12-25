import numpy as np
import gensim.downloader
from Levenshtein import distance

import nltk
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
from nltk import pos_tag, word_tokenize
from nltk.stem.snowball import SnowballStemmer

def create_similarity_algorithm():
    return gensim.downloader.load('word2vec-ruscorpora-300')

def compute_similarity(algorithm, word_1, word_2):
    return algorithm.similarity(f'{word_1}_NOUN', f'{word_2}_NOUN')

def create_stemmer():
    return SnowballStemmer("russian")

def stem_word(stemmer, word):
    return stemmer.stem(word)

def compute_category_similarity(source_category, categories, algorithm, stemmer):

    category_similarity = dict()

    for category_id, category in categories:

        word_1 = source_category[0].lower()
        word_2 = category.lower()

        tokenized_text = word_tokenize(word_1, language='russian')
        tagged = pos_tag(tokenized_text)

        similarity = []
        for word, tag in tagged:
            if tag not in ['NNP']:

                # Посимвольное сравнение категорий, вычисление количества отличающихся символов
                ldistance = distance(word, word_2)
                lsim = (len(word_2) - ldistance) / len(word_2)
                if lsim >= 0.7:
                    similarity.append(lsim)
                    continue

                try:
                    similarity.append(compute_similarity(algorithm, word, word_2))
                except KeyError:
                    try:
                        word = stem_word(stemmer, word)
                        similarity.append(compute_similarity(algorithm, word, word_2))
                    except KeyError:
                        similarity.append(0)

        mean_similarity = np.mean(similarity)
        category_similarity[category_id] = [mean_similarity]

    return category_similarity
