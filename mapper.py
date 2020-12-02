#!/usr/bin/env python

import glob
import os
import sys
from pyarrow import parquet
from nltk import word_tokenize
from nltk.corpus import stopwords
spanishStopWords = stopwords.words('spanish')


def category_from_filename(filename):
    """
    Extracts the category from the filename, e.g. if filename is MLA1055_reviews_page0.parquet it should return MLA1055

    :param filename: name of the file
    :return: extracted category
    """
    # TODO: implementar
    extension = filename.split("MLA")[1]
    return "MLA" + extension[:4]


class Review:
    """
    Models an Item's review.
    """
    def __init__(self, key, category, title, content, rate, likes, dislikes):
        self.key = key
        self.category = category
        self.title = title
        self.content = content
        self.rate = rate
        self.likes = likes
        self.dislikes = dislikes

    @classmethod
    def from_record(cls, record, filename):
        """
        Constructs a Review from the contents of a record and the filename it belongs to

        :param record: Series with the attributes of a Review
        :param filename: file to which the record belongs to
        :return: an instance of Review
        """
        category = category_from_filename(filename)
        # TODO: reemplazar o ajustar el resto de este método para que extraiga las características del review
        #       según los nombres de columnas que hay en sus archivos parquet
        return Review(
            key=record['review_key'],
            category=category,
            title=record['review_title'],
            content=record['review_content'],
            rate=record['review_rate'],
            likes=record['review_likes'],
            dislikes=record['review_dislikes']
        )


def accept(term):
    """
    Acceptance criteria for a term

    :param term: term to accept or not
    :return: True if the term is accepted, False otherwise
    """
    # TODO implementar acá un filtro que devuelve True sólo para las palabras de interés,
    #      por ejemplo, eliminando STOPWORDS o filtrando sólo palabras alfanuméricas.
    return (not term in spanishStopWords) & term.isalnum()


def sanitize(word):
    """
    Cleans and normalizes the word

    :param word: word to sanitize
    :return: sanitized word
    """
    # TODO: normalizar la palabra aquí, por ejemplo pasar a minúsculas y eliminar espacios extra
    word = word.lower()
    replacement = {
        " " : "",
        "á" : "a",
        "é" : "e",
        "í" : "i",
        "ó" : "o",
        "ú" : "u"
    }
    for key, value in replacement.items():
        word = word.replace(key, value)
    return word


def get_score_bucket(score, threshold=3):
    """
    Binarizes the score into "negative" and "positive" buckets

    :param score: score to binarize
    :param threshold: threshold below which the score is considered negative and above positive
    :return: the bucket
    """
    if score <= threshold:
        return "negative"
    else:
        return "positive"


def map_function(content, category, bucket):
    """
    Map part of MapReduce

    :param content: the content of a review
    :param category: the category to which the review belongs
    :param bucket: the bucket to which the review belongs ("negative" or "positive")
    :return: a list of (key, value) pairs to be shuffled and reduced later on.
    """
    out = []
    for word in word_tokenize(content):
        term = sanitize(word)
        if not accept(term):
            continue
        key = term + "\t" + category + "\t" + bucket
        value = 1
        out.append((key, value))

    return out


def main():
    output = []
    inputfiles = sys.argv[1:]
    if len(inputfiles) == 1:
        inputfiles = glob.glob(inputfiles[0])

    for inputfile in inputfiles:
        records = parquet.read_pandas(inputfile).to_pandas()
        for index, record in records.iterrows():
            review = Review.from_record(record, os.path.basename(inputfile))
            output.extend(map_function(review.content, review.category, get_score_bucket(review.rate)))

    # The (key, value) pairs are output in sorted order by key
    for key, value in sorted(output):
        print(key + "\t" + str(value))



if __name__ == '__main__':
    main()
