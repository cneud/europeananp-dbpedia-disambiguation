#!/usr/bin/env python

import sys

import fuzzycomp
import json
import math
import re
import solr

LANG = "en"

s = solr.SolrConnection('http://localhost:8984/solr/dbpedia_' + LANG)

CUTOFF_RELEVANCY = 0.0
CUTOFF_SIMILARITY = 0.6
CUTOFF_TOTAL_SCORE = 0.02


def _escape(toEscape):
    replaceCharacter = ["+", "-", "&&", "||", "!", "(", ")", "{",
                        "}", "[", "]", "^", "\"", "~", "*", "?", ":"]

    cleaned = toEscape.rstrip().lstrip()

    for c in replaceCharacter:
        cleaned = cleaned.replace(c, '\\' + c)
    return cleaned


def _cleanedLabel(label):
    '''remove information in parenthesis, lowercase'''
    return re.sub(r'\(.*\)', '', label.lower()).rstrip().lstrip()


def _stringSimilarity(a, b):
    '''adapted string similarity: combines jaro-winkler distance with
       number of common terms in both strings '''

    if a and b and len(a) > 0 and len(b) > 0:
        sa = set(''.join([c for c in a.split(" ")]))
        sb = set(''.join([c for c in b.split(" ")]))

        intersect = sa.intersection(sb)
        if (sa and sb and len(sa) > 0 and len(sb) > 0):
            jaro = fuzzycomp.jaro_winkler(a.encode('utf-8'),
                                          b.encode('utf-8'), 0.1)
            jaro *= float(len(intersect)) / float(max(len(sa), len(sb)))
            return jaro
        else:
            return 0.0
    else:
        return 0.0


def disambiguateList(entityStrings):
    result = dict()
    for s in set(entityStrings):
        result[s] = linkEntity(s)
    return result


def linkEntity(namedEntityString):
    cleaned = _escape(namedEntityString.decode('utf-8').lower())

    prefix = "label_" + LANG + ":"
    labelQuery = "label_" + LANG + ":\"" + cleaned + "\"^2000 "
    labelQuery += " ".join([prefix + elt for elt in cleaned.split(" ")])

    prefix = "redirectLabel:"
    redirectLabelQuery = "redirectLabel:\"" + cleaned + "\"^2000 "
    redirectLabelQuery += " ".join([prefix + e for e in cleaned.split(" ")])

    query = "((" + labelQuery + ") OR (" + redirectLabelQuery + "))"
    query += " AND _val_:inlinks^10"
    query += " AND (schemaorgtype:Person^10 OR"
    query += " schemaorgtype:Place OR"
    query += " schemaorgtype:Organization)"

    filter_query = "schemaorgtype:Person OR"
    filter_query += " schemaorgtype:Place OR"
    filter_query += " schemaorgtype:Organization"

    try:
        result = s.raw_query(q=query, fq=filter_query,
                             fl="* score", rows=5, indent="on", wt="json")
    except Exception:
        return None

    bestMatch = None
    bestMatchMainLabel = None

    jsonResult = json.loads(result)
    maxScore = jsonResult["response"]["maxScore"]

    score = -1.0
    sumScore = 0.0

    sum_labels = dict()
    main_labels = dict()

    for d in jsonResult["response"]["docs"]:
        if (d.get("score")/maxScore) > CUTOFF_RELEVANCY:
            sumScore += d.get("score")
            labels = d.get("redirectLabel")

            if labels is None:
                labels = []

            main_labels[d.get("id")] = d.get("label_" + LANG)
            sum_labels[d.get("id")] = [(_cleanedLabel(d.get("label_" + LANG)),
                                       d.get("score"))]
        for l in labels:
            sum_labels[d.get("id")].append((_cleanedLabel(l), d.get("score")))

    for d in sum_labels.keys():
        for l in sum_labels.get(d):
            sim_score = _stringSimilarity(cleaned, l[0])
            if sim_score > CUTOFF_SIMILARITY:
                relativeRelevancyScore = l[1] / sumScore
                labelScore = sim_score * math.sqrt(relativeRelevancyScore)
                if labelScore > score:
                    bestMatch = d
                    bestMatchMainLabel = main_labels[d]
                    score = labelScore

    if score > CUTOFF_TOTAL_SCORE:
        return bestMatch, score, bestMatchMainLabel
    else:
        return None, -1.0, bestMatchMainLabel

if __name__ == '__main__':
    print(linkEntity(sys.argv[1]))
