from data import *
import pandas as pd

def look_forward(sent, ind, start=None, end=None):
    ngrams = []
    for length in range(1, len(sent) - ind):
        if start is None and end is None:
            ngrams.append(sent[ind+1: ind+length+1])
        elif start is not None and sent[ind+1] == start:
            ngrams.append(sent[ind+1: ind+length+1])
        elif end is not None and sent[ind+length+1] == end:
            ngrams.append(sent[ind+1: ind+length+1])
    return ngrams

def look_backward(sent, ind, start=None, end=None):
    ngrams = []
    for length in range(1, ind+1):
        if start is None and end is None:
            ngrams.append(sent[ind-length: ind])
        elif start is not None and sent[ind-length] == start:
            ngrams.append(sent[ind - length: ind])
        elif end is not None and sent[ind] == end:
            ngrams.append(sent[ind - length: ind])
    return ngrams

# extract host, equal weights for all methods
ca_set = []
for tweet in gg2013:
    sent = tweet['text'].split()

    for i, word in enumerate(sent):
        # hosted by xxxxxxxx
        if word == 'hosted' and i+1 < len(sent) and sent[i+1] == 'by':
            ca_set += look_forward(sent, i+1)
        # xxxxxxxxx host(s)
        elif word == 'host' or word == 'hosts':
            ca_set += look_backward(sent, i)
        elif word == 'hosts' and i > 1 and sent[i-1] == 'the' and sent[i-2] == 'are':
            ca_set += look_backward(sent, i-2)
        elif  word == 'hosts' and i > 1 and sent[i-2] == 'are':
            ca_set += look_backward(sent, i-1)
        elif word == 'host' and i > 1 and sent[i-1] == 'the' and sent[i-2] == 'is':
            ca_set += look_backward(sent, i-2)
        # xxxxxxxxx is/are hosting
        elif word == 'hosting' and i > 0 and (sent[i-1] == 'is' or sent[i-1] == 'are'):
            ca_set += look_backward(sent, i-1)
        elif word == 'hosting' and i > 0 and sent[i-1] != 'is' and sent[i-1] != 'are':
            ca_set += look_backward(sent, i)

# merge and count
def unique_ngrams(ngrams_lst):
    uni = []
    cnt = []
    for ngram in ngrams_lst:
        if ngram not in uni:
            uni.append(ngram)
    for uni_ngram in uni:
        cnt.append([uni_ngram, ngrams_lst.count(uni_ngram)])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt

def is_Sublist(l, s):
    sub_set = False
    if s == []:
        sub_set = True
    elif s == l:
        sub_set = True
    elif len(s) > len(l):
        sub_set = False
    else:
        l_string = ' '.join(l)
        s_string = ' '.join(s)
        sub_set = s_string in l_string
    return sub_set

# l and s are both string splits
def is_StrictSublist(l, s):
    sub_set = False
    if s == []:
        sub_set = True
    elif len(s) >= len(l):
        sub_set = False
    else:
        l_string = ' '.join(l)
        s_string = ' '.join(s)
        sub_set = s_string in l_string
    return sub_set

def remove_duplicate_sublist(sorted_ca):
    removal = []
    for ca1 in sorted_ca:
        for ca2 in sorted_ca:
            if is_StrictSublist(ca2[0], ca1[0]) and ca1[1] <= ca2[1]:
                if ca1 not in removal:
                    removal.append(ca1)
    for to_remove in removal:
        sorted_ca.remove(to_remove)
    return sorted_ca

def remove_all_sublists(sorted_ca):
    removal = []
    for ca1 in sorted_ca:
        for ca2 in sorted_ca:
            if is_StrictSublist(ca2[0], ca1[0]):
                if ca1 not in removal:
                    removal.append(ca1)
    for to_remove in removal:
        sorted_ca.remove(to_remove)
    return [x[0] for x in sorted_ca]

sorted_ca = unique_ngrams(ca_set)[:50] #select only top fifty
sorted_ca = remove_duplicate_sublist(sorted_ca)

# remove all sublists at this point, re-rank based on occurrence frequency
ca = remove_all_sublists(sorted_ca)

ca_freq = []
for x in ca:
    n = 0
    for tweet in gg2013:
        sent = tweet['text'].split()
        if is_Sublist(sent, x):
            n += 1
    ca_freq.append([x, n])

sorted_ca_freq = sorted(ca_freq, key=lambda x :x[1], reverse=True)

found = False
while not found:
    if sorted_ca_freq[0][0] == ['the'] or sorted_ca_freq[0][0] == ['golden', 'globes'] or sorted_ca_freq[0][0] == ['to']:
        sorted_ca_freq.remove(sorted_ca_freq[0])
    else:
        print('Host(s): ' + ' '.join(sorted_ca_freq[0][0]))
        found = True

# extract award names
ca_set_awards = []
for tweet in gg2013:
    sent = tweet['text'].split()

    for i, word in enumerate(sent):
        # best xxx goes to
        if word == 'goes' and i+1 < len(sent) and sent[i+1] == 'to':
            ca_set_awards += look_backward(sent, i, start='best')
        # xxx won best xxx
        elif word == 'won':
            ca_set_awards += look_forward(sent, i, start='best')
        # best xxx - *winner
        # elif word == '-':
        #     ca_set_awards += look_backward(sent, i, start='best')

sorted_ca_awards = unique_ngrams(ca_set_awards)[:80] #select only top eighty
sorted_ca_awards = remove_duplicate_sublist(sorted_ca_awards)

# remove all sublists at this point, re-rank based on occurrence frequency
ca_awards = remove_all_sublists(sorted_ca_awards)

ca_freq_awards = []
for x in ca_awards:
    n = 0
    for tweet in gg2013:
        sent = tweet['text'].split()
        if is_Sublist(sent, x):
            n += 1
    ca_freq_awards.append([x, n])

sorted_ca_freq_awards = sorted(ca_freq_awards, key=lambda x :x[1], reverse=True)

# Print list of awards:

# find nominees, presenters, and winners for awards
