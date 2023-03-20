import json
import re
import unidecode

gg2013 = json.load(open('./gg2013.json'))
# gg2015 = json.load(open('./gg2015.json'))


def clean(data):
    for tweet in data:
        sent = tweet['text'].strip().split()
        tweet['hashtag'] = []
        new_sent = []
        for i, word in enumerate(sent):
            # not good for extracting winners
            '''if word.startswith('#'):
                # store hashtags separately
                tweet['hashtag'].append(word[1:].lower())'''
            if word.startswith('#'):
                # store hashtags separately
                new_sent.append(word[1:].lower())
            elif word == '&amp;':
                # replace & with and
                new_sent.append('and')
            elif word.startswith('@') and i > 0 and sent[i - 1] == 'RT':
                new_sent = []
            else:
                # remove punctuation and case
                new_sent.append(word.lower())
        tweet['text'] = unidecode.unidecode(re.sub(r'[^\w\s]', '', " ".join(new_sent)))


clean(gg2013)
