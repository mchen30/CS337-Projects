import re
import ray
import pandas as pd
import unidecode
from functools import reduce


@ray.remote
def clean(data, indices):
    texts = []
    sents = [x.split() for x in data.iloc[range(*indices)].tolist()]
    for sent in sents:
        new_sent = []
        for i, word in enumerate(sent):
            # not good for extracting winners
            if word.startswith('#') or word.startswith('http'):
                continue
            elif word == '&amp;':
                # replace & with and
                new_sent.append('and')
            elif '...' in word:
                new_sent.append(word.replace('...', ' ').lower())
            elif word.startswith('@') and i > 0 and sent[i - 1] == 'RT':
                new_sent = []
            elif word.startswith('@') and word.lower() != '@goldenglobes':
                continue
            else:
                # remove punctuation and case
                new_sent.append(word.lower())
        texts.append(unidecode.unidecode(re.sub(r'[^\w\s]', '', " ".join(new_sent))))
    return texts


def load(path, n_CPU):
    data = pd.read_json(path, orient='records', dtype={'text': 'object', 'timestamp_ms': 'int64',
                                                       'user': 'bool', 'id': 'bool'})
    data_ref = ray.put(data.text)
    text_refs = [clean.remote(data_ref, [int(len(data) / n_CPU * cpu), int(len(data) / n_CPU * (cpu+1))]) for cpu in range(n_CPU)]
    text = reduce(lambda x, y: x+y, [ray.get(ref) for ref in text_refs])
    new_data = pd.DataFrame(data={'text': text, 'timestamp_ms': data.timestamp_ms})
    return new_data


award_lst = [['best', 'motion', 'picture', 'drama'],
          ['best', 'motion', 'picture', 'comedy', 'or', 'musical'],
          ['best', 'actor', 'in', 'a', 'motion', 'picture', 'drama'],
          ['best', 'actress', 'in', 'a', 'motion', 'picture', 'drama'],
          ['best', 'actor', 'in', 'a', 'motion', 'picture', 'comedy', 'or', 'musical'],
          ['best', 'actress', 'in', 'a', 'motion', 'picture', 'comedy', 'or', 'musical'],
          ['best', 'supporting', 'actor', 'in', 'a', 'motion', 'picture'],
          ['best', 'supporting', 'actress', 'in', 'a', 'motion', 'picture'],
          ['best', 'director', 'motion', 'picture'],
          ['best', 'screenplay'],
          ['best', 'original', 'score'],
          ['best', 'original', 'song', 'motion', 'picture'],
          ['best', 'animated', 'feature', 'film'],
          ['best', 'foreign', 'film'],
          ['best', 'tv', 'series', 'drama'],
          ['best', 'tv', 'series', 'comedy', 'or', 'musical'],
          ['best', 'actor', 'in', 'a', 'tv', 'series', 'drama'],
          ['best', 'actress', 'in', 'a', 'tv', 'series', 'drama'],
          # comedy/musical should map to comedy or musical
          ['best', 'actor', 'in', 'a', 'tv', 'series', 'comedy', 'or', 'musical'],
          ['best', 'actress', 'in', 'a', 'tv', 'series', 'comedy', 'or', 'musical'],
          # miniseries/tv movie should map to miniseries or tv movie
          ['best', 'actor', 'in', 'a', 'miniseries', 'or', 'tv', 'movie'],
          ['best', 'actress', 'in', 'a', 'miniseries', 'or', 'tv', 'movie'],
          ['best', 'supporting', 'actor', 'in', 'a', 'tv', 'series'],
          ['best', 'supporting', 'actress', 'in', 'a', 'tv', 'series'],
          ['best', 'miniseries', 'or', 'tv', 'movie']]

award_map = {'best screenplay - motion picture': ['best', 'screenplay'],
             'best director - motion picture': ['best', 'director', 'motion', 'picture'],
             'best performance by an actress in a television series - comedy or musical': ['best', 'actress', 'in', 'a', 'tv', 'series', 'comedy', 'or', 'musical'],
             'best foreign language film': ['best', 'foreign', 'film'],
             'best performance by an actor in a supporting role in a motion picture': ['best', 'supporting', 'actor', 'in', 'a', 'motion', 'picture'],
             'best performance by an actress in a supporting role in a series, mini-series or motion picture made for television': ['best', 'supporting', 'actress', 'in', 'a', 'tv', 'series'],
             'best motion picture - comedy or musical': ['best', 'motion', 'picture', 'comedy', 'or', 'musical'],
             'best performance by an actress in a motion picture - comedy or musical': ['best', 'actress', 'in', 'a', 'motion', 'picture', 'comedy', 'or', 'musical'],
             'best mini-series or motion picture made for television': ['best', 'miniseries', 'or', 'tv', 'movie'],
             'best original score - motion picture': ['best', 'original', 'score'],
             'best performance by an actress in a television series - drama': ['best', 'actress', 'in', 'a', 'tv', 'series', 'drama'],
             'best performance by an actress in a motion picture - drama': ['best', 'actress', 'in', 'a', 'motion', 'picture', 'drama'],
             'best performance by an actor in a motion picture - comedy or musical': ['best', 'actor', 'in', 'a', 'motion', 'picture', 'comedy', 'or', 'musical'],
             'best motion picture - drama': ['best', 'motion', 'picture', 'drama'],
             'best performance by an actor in a supporting role in a series, mini-series or motion picture made for television': ['best', 'supporting', 'actor', 'in', 'a', 'tv', 'series'],
             'best performance by an actress in a supporting role in a motion picture': ['best', 'supporting', 'actress', 'in', 'a', 'motion', 'picture'],
             'best television series - drama': ['best', 'tv', 'series', 'drama'],
             'best performance by an actor in a mini-series or motion picture made for television': ['best', 'actor', 'in', 'a', 'miniseries', 'or', 'tv', 'movie'],
             'best performance by an actress in a mini-series or motion picture made for television': ['best', 'actress', 'in', 'a', 'miniseries', 'or', 'tv', 'movie'],
             'best animated feature film': ['best', 'animated', 'feature', 'film'],
             'best original song - motion picture': ['best', 'original', 'song', 'motion', 'picture'],
             'best performance by an actor in a motion picture - drama': ['best', 'actor', 'in', 'a', 'motion', 'picture', 'drama'],
             'best television series - comedy or musical': ['best', 'tv', 'series', 'comedy', 'or', 'musical'],
             'best performance by an actor in a television series - drama': ['best', 'actor', 'in', 'a', 'tv', 'series', 'drama'],
             'best performance by an actor in a television series - comedy or musical': ['best', 'actor', 'in', 'a', 'tv', 'series', 'comedy', 'or', 'musical'],
             'cecil b. demille award': ['cecil', 'b.', 'demille', 'award']}

award_map_inv = {}
for k in award_map.keys():
    award_map_inv[' '.join(award_map[k])] = k
