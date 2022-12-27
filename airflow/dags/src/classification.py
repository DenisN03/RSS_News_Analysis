import numpy as np
import gensim.downloader
from Levenshtein import distance

import nltk
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
from nltk import pos_tag, word_tokenize
from nltk.stem.snowball import SnowballStemmer

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    Doc
)

def create_similarity_algorithm():
    return gensim.downloader.load('word2vec-ruscorpora-300')

def compute_similarity(algorithm, word_1, word_2):
    return algorithm.similarity(f'{word_1}_NOUN', f'{word_2}_NOUN')

def create_stemmer():
    return SnowballStemmer("russian")

def stem_word(stemmer, word):
    return stemmer.stem(word)

def prepare_lemmator():
    segmenter = Segmenter()
    morph_vocab = MorphVocab()

    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)

    return segmenter, morph_vocab, morph_tagger

def perform_lemmatization(word, segmenter, morph_vocab, morph_tagger):
    doc = Doc(word)

    # Segmentation
    doc.segment(segmenter)

    # Morphology
    doc.tag_morph(morph_tagger)

    # Lemmatization
    for token in doc.tokens:
        token.lemmatize(morph_vocab)

    return doc.tokens[0].lemma

def compute_category_similarity(source_category, categories, algorithm, stemmer, segmenter, morph_vocab, morph_tagger):

    category_similarity = dict()

    for category_id, category in categories:

        tokenized_text = nltk.word_tokenize(source_category[0], language='russian')
        tagged = nltk.pos_tag(tokenized_text)

        similarity = []
        for word, tag in tagged:
            if tag not in ['NNP']:

                # Лемматизация категории источника
                word = perform_lemmatization(word, segmenter, morph_vocab, morph_tagger)

                # Посимвольное сравнение категорий, вычисление количества отличающихся символов
                ldistance = distance(word, category)
                lsim = (len(category) - ldistance) / len(category)
                if lsim >= 0.7:
                    similarity.append(lsim)
                    continue

                try:
                    similarity.append(compute_similarity(algorithm, word, category))
                except KeyError:
                    try:
                        word = stem_word(stemmer, word)
                        similarity.append(compute_similarity(algorithm, word, category))
                    except KeyError:
                        similarity.append(0)

        mean_similarity = np.mean(similarity)
        category_similarity[category_id] = [mean_similarity]

    return category_similarity
