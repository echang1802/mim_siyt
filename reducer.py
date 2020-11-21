#!/usr/bin/env python

import csv
import math
import sys


def reduce_function(buffer, totals):
    """
    Reduce part of MapReduce

    :param buffer: list of (key, value) to reduce (here key is formed by the term, category and bucket).
    :param totals: dictionary where the reduced key,values are collected
    :return: nothing
    """
    # TODO aquí deben recorrer los términos, categorías, buckets y valores que hay en el buffer
    #       y por cada uno de ellos actualizar el diccionario totals.
    #       totals es un diccionario donde la clave es cada bucket, y el valor es un diccionario
    #       de categorías a un diccionario de términos y cantidades:
    #       totals[bucket][category][term] está asociado a un número entero
    for (term, category, bucket, value) in buffer:
        if not bucket in totals.keys():
            totals[bucket] = {}
        if not category in totals[bucket].keys():
            totals[bucket][category] = {}
        totals[bucket][category][term] = value

def calculate_tfs(totals):
    """
    Calculates the term frequencies and creates inverse indexes mapping terms to buckets and to categories

    :param totals: Reduced key,values
    :return: a tuple consisting of (term frequencies, map of term:buckets, map of term:categories)
    """
    tfs = {}
    term_buckets = {}
    term_categories = {}

    for bucket, bucket_totals in totals.items():
        tfs[bucket] = {}
        for category, category_totals in bucket_totals.items():
            tfs[bucket][category] = {}
            for term, frequency in category_totals.items():
                tfs[bucket][category][term] = frequency / sum(category_totals.values())

                if term not in term_buckets:
                    term_buckets[term] = set()
                term_buckets[term].add(bucket)

                if term not in term_categories:
                    term_categories[term] = set()
                term_categories[term].add(category)

    return tfs, term_buckets, term_categories


def calculate_tfs_idfs(tfs, term_buckets, term_categories):
    """
    Calculates a modified version of TF-IDF that better suits our problem

    :param tfs: term frequencies
    :param term_buckets: map term:buckets
    :param term_categories: map term:categories
    :return: the modified TF-IDF scores of each term per (category, bucket)
    """
    tf_idfs = {}
    n_buckets = len(tfs)
    n_categories = len(list(tfs.values())[0])
    for bucket, bucket_tfs in tfs.items():
        tf_idfs[bucket] = {}
        for category, category_tfs in bucket_tfs.items():
            tf_idfs[bucket][category] = {}
            for term, tf in category_tfs.items():
                # TODO aquí deben calcular el tfs/idfs del bucket, categoría y término y volcarlo en el diccionario tf_idfs
                idf = math.log(n_categories / term_categories[term].len())
                tf_idfs[bucket][category][term] = tf * idf

    return tf_idfs


def merge(totals):
    """
    Merge function to be applied after reducing all key,values

    :param totals: reduced key,values
    :return: the result of merging all the reduced keys
    """
    tfs, term_buckets, term_categories = calculate_tfs(totals)

    return calculate_tfs_idfs(tfs, term_buckets, term_categories)


def write_csv(category, bucket, sorted_scores):
    """
    Writes the terms and TF-IDF scores corresponding to a (category, bucket) to a csv file.

    :param category: category
    :param bucket: bucket ("negative" or "positive")
    :param sorted_scores: list of (term, score) sorted by score from highest to lowest
    """
    with open("terms_" + category + "_" + bucket + ".csv", 'w') as f:
        writer = csv.writer(f)
        writer.writerow(('Término', 'TF/IDF'))
        writer.writerows(sorted_scores)


def main():
    totals = {}
    cur_key = None
    buffer = []

    for input_line in sys.stdin:
        term, category, bucket, value = input_line.strip().split("\t")
        if cur_key == (term, category):
            buffer.append((term, category, bucket, int(value)))
        else:
            cur_key = (term, category)
            if buffer:
                reduce_function(buffer, totals)
                buffer = []
            else:
                buffer.append((term, category, bucket, int(value)))

    tf_idfs = merge(totals)

    for bucket, bucket_tf_idfs in tf_idfs.items():
        for category, category_tf_idfs in bucket_tf_idfs.items():
            sorted_scores = sorted(category_tf_idfs.items(), key=lambda x: x[1], reverse=True)
            write_csv(category, bucket, sorted_scores)



if __name__ == '__main__':
    main()
