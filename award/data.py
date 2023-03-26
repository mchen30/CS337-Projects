import json
import re
import unidecode

gg2013 = json.load(open('./gg2013.json'))
gg2015 = json.load(open('./gg2015.json'))


def clean(data):
    for tweet in data:
        sent = tweet['text'].strip().split()
        tweet['hashtag'] = []
        new_sent = []
        for i, word in enumerate(sent):
            # not good for extracting winners
            if word.startswith('#'):
                # store hashtags separately
                tweet['hashtag'].append(word[1:].lower())
                '''if word.startswith('#'):
                # store hashtags separately
                new_sent.append(word[1:].lower())'''
            elif word.startswith('http'):
                continue
            elif word == '&amp;':
                # replace & with and
                new_sent.append('and')
            elif word.startswith('@') and i > 0 and sent[i - 1] == 'RT':
                new_sent = []
            elif word.startswith('@') and word.lower() != '@goldenglobes':
                continue
            else:
                # remove punctuation and case
                new_sent.append(word.lower())
        tweet['text'] = unidecode.unidecode(re.sub(r'[^\w\s]', '', " ".join(new_sent)))


clean(gg2013)
clean(gg2015)

awards = [['best', 'motion', 'picture', 'drama'],
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
             'best performance by an actor in a television series - comedy or musical': ['best', 'actor', 'in', 'a', 'tv', 'series', 'comedy', 'or', 'musical']}

