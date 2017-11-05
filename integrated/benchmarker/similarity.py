import numpy as np
from gensim.corpora import Dictionary
from gensim.models import ldamodel
from gensim import similarities
from gensim.models.phrases import Phrases, Phraser
from spacy.en.language_data import STOP_WORDS
import spacy
from gensim.models import Word2Vec


# to identify each noun phrase as a keyword/phrase to be compared for similarities
def get_keywords(research):
    ls = []
    research_words = list(map(lambda x: x.pos_, research))
    i = len(research) - 1

    while i >= 0:
        if research_words[i] in ["PROPN", "NOUN"]:
            j = i - 1
            while j >= 0 and research_words[j] in ["PROPN", "ADJ"]:
                j -= 1
            ls.append(research[j + 1:i + 1])
            i = j
        i = i - 1
    return ls


# function to remove stop words and separate each sentence into a list
def separate_sentence(sentences):
    result = list()
    for sentence in sentences.sents:
        result.append(
            list(token.lemma_ for token in sentence if token.text not in STOP_WORDS and token.is_punct == False))
    return result


# to get an average similarity score of keyword_2 as compared to keyword_1
def get_similarity(keyword_1, keyword_2):
    score = []
    if len(keyword_2) > 0:  # if there are multiple research area
        for n in keyword_1:
            s = []
            for i in keyword_2:
                if len(i) > 0:
                    s.append(n.similarity(i))
            score.append(max(s))
    score = list(filter(lambda x: x != 0, score))
    if len(score) > 0:
        return sum(score) / len(score)
    return 0

def get_n_avg(ls, n):
    assert n <= len(ls)
    total = sum(sorted(ls, reverse = False)[:n])
    return float(total) / float(n)


# calculate the cosine similarities between the different sentences in each row
# if the research area text contains more than 1 sentence, compare the sentence by pair for all combinations
# and take the max score
def get_n_similarity(sentences_1, sentences_2, model):
    score = 0
    for s1 in sentences_1:
        for s2 in sentences_2:
            try:
                temp_score = model.wv.n_similarity(s1, s2)
                if temp_score > score:
                    score = temp_score
            except ZeroDivisionError:
                pass
    return score


class Similarity:
    def __init__(self, data, seed = 124):
        self.data = data.copy()
        self.data["text_raw"] = self.data["text_raw"].apply(lambda x: x if x != "Unknown" else u"")
            #lambda x: '. '.join(y.lstrip('()0123456789.-') for y in x.split('\n')) if x != "Unknown" else "")
        self.nlp = spacy.load("en_core_web_md")
        self.seed = seed
        self.lda_score = False
        self.keyword_score = False
        self.word2vec_score = False

    def add_nus_info(self, nus_info):
        self.data = self.data.append(nus_info)

    def get_data(self):
        return self.data.copy()

    def apply_nlp(self):
        self.data["nlp"] = self.data["text_raw"].apply(self.nlp)

    def apply_bigram(self):
        if "nlp" not in self.data.columns.tolist():
            self.apply_nlp()
        self.data["sentences"] = self.data["nlp"].apply(separate_sentence)
        phrases = Phrases(self.data["sentences"].sum())
        bigram = Phraser(phrases)
        self.data["sentences_bigram"] = self.data["sentences"].apply(
            lambda doc: [bigram[sentence] for sentence in doc])

    def get_nlp_score(self):
        if self.keyword_score == False:
            self.data["keywords"] = self.data["nlp"].apply(get_keywords)
            # compare the similarities of each row to the first row
            self.data["keyword_score"] = self.data["keywords"].apply(
                lambda x: get_similarity(self.data[self.data["tag"].isnull()]["keywords"][0], x))
            self.keyword_score = True


    def get_word2vec_score(self):
        if self.word2vec_score == False:
            self.apply_bigram()
            model = Word2Vec(self.data["sentences_bigram"].sum(), min_count=1, iter=200)
            # use gensim phrases and phrases to model bigram
            self.data["word2vec_score"] = self.data["sentences_bigram"].apply(
                lambda x: get_n_similarity(self.data[self.data["tag"].isnull()]["sentences_bigram"][0], x, model))
            self.word2vec_score = True

    def get_lda_score(self):
        if self.lda_score == False:
            if "sentences_bigram" not in self.data.columns.tolist():
                self.apply_bigram()
            dictionary = Dictionary([sum(sentences, []) for sentences in self.data["sentences_bigram"]])
            corpus = [dictionary.doc2bow(sum(sentences, [])) for sentences in self.data["sentences_bigram"]]

            np.random.seed(self.seed)  # setting random seed to get the same results each time.
            model_lda = ldamodel.LdaModel(corpus, id2word = dictionary, num_topics=25)

            doc = dictionary.doc2bow(sum(self.data[self.data["tag"].isnull()]["sentences_bigram"][0], []))
            doc_lda = model_lda[doc]
            # find the most similar documents from all the documents in the corpus
            index = similarities.MatrixSimilarity(model_lda[corpus])
            # print similarities (cosine similarity)
            self.data["lda_score"] = index[doc_lda]
            self.lda_score = True

    def get_all_scores(self):
        self.get_lda_score()
        self.get_nlp_score()
        self.get_word2vec_score()

    def scores(self):
        cols = []
        if self.lda_score:
            cols.append("lda_score")
        if self.word2vec_score:
            cols.append("word2vec_score")
        if self.keyword_score:
            cols.append("keyword_score")
        return cols

    def get_avg_score(self, scores = "top2"):
        self.get_all_scores()
        score_cols = self.scores()
        col = "research_area_score"
        self.data["score_list"] = self.data[score_cols].values.tolist()
        if scores == "all":
            self.data[col] = self.data["score_list"].apply(lambda x:get_n_avg(x, 3))
        else:# scores == "TOP2":
            self.data[col] = self.data["score_list"].apply(lambda x:get_n_avg(x, 2))
        return self.data[self.data["tag"].notnull()]




