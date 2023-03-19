import json
import re

gg2013 = json.load(open('../gg2013.json'))
# gg2015 = json.load(open('../gg2015.json'))


def clean(data):
    for tweet in data:
        sent = tweet['text'].strip().split()
        tweet['hashtag'] = []
        new_sent = []
        for word in sent:
            if word.startswith('#'):
                # store hashtags separately
                tweet['hashtag'].append(word[1:])
            elif word == '&amp;':
                # replace & with and
                new_sent.append('and')
            else:
                # remove punctuation and case
                new_sent.append(word.lower())
        tweet['text'] = re.sub(r'[^\w\s]', '', " ".join(new_sent))


clean(gg2013)
