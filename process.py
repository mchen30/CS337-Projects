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

sorted_ca = unique_ngrams(ca_set)[:50]  # select only top fifty
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
        elif word == 'won' and i > 0 and sent[i-1] == 'has':
            ca_set_awards += look_forward(sent, i-1, start='best')
        elif word == 'won':
            ca_set_awards += look_forward(sent, i, start='best')
        # best xxx - *winner
        # elif word == '-':
        #     ca_set_awards += look_backward(sent, i, start='best')

sorted_ca_awards = unique_ngrams(ca_set_awards)[:90] #select only top eighty
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
awards = [['best', 'motion', 'picture', 'drama'],
          ['best', 'motion', 'picture', 'comedy', 'or', 'musical'],
          ['best', 'actor', 'in', 'a', 'motion', 'picture', 'drama'],
          ['best', 'actress', 'in', 'a', 'motion', 'picture', 'drama'],
          ['best', 'supporting', 'actor', 'in', 'a', 'motion', 'picture'],
          ['best', 'supporting', 'actress', 'in', 'a', 'motion', 'picture'],
          ['best', 'director', 'motion', 'picture'],
          ['best', 'screenplay'],
          ['best', 'original', 'score'],
          ['best', 'original', 'song', 'motion', 'picture'],
          ['best', 'song'],
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
          ['best', 'actor', 'in', 'a', 'miniseries', 'tv', 'movie'],
          ['best', 'actress', 'in', 'a', 'miniseries', 'tv', 'movie'],
          ['best', 'supporting', 'actor', 'in', 'a', 'tv', 'series'],
          ['best', 'supporting', 'actress', 'in', 'a', 'tv', 'series'],
          ['best', 'miniseries', 'or', 'tv', 'movie']]

# xxxx is presenting AWARD

# xxx (has) won AWARD
# xxx has won the golden globe for AWARD
# xxx ... for her golden globe win as AWARD

# someone presents AWARD to xxx

sorted_ca_freq_winners = []
sorted_ca_freq_presenters = []
sorted_ca_freq_nominees = []

for i, award in enumerate(awards):
    # extract award names
    award_str = ' '.join(award)
    ca_set_winner = []
    for tweet in gg2013:
        sent = tweet['text'].split()
        for i, word in enumerate(sent):
            sent_str = ' '.join(sent[i:])
            # AWARD: xxx
            if award_str in sent_str and i + len(award) < len(sent):
                ca_set_winner += look_forward(sent, i+len(award))
            # AWARD goes to xxx
            elif award_str in sent_str and i + len(award) + 2 < len(sent) and sent[i + len(award)] == 'goes' and sent[i + len(award) + 1] == 'to':
                ca_set_winner += look_forward(sent, i+len(award)+2)
            # xxx - #gg winner for AWARD
            elif award_str in sent_str and i > 2:
                ca_set_winner += look_backward(sent, i - 2)
            # winner for AWARD is xxx
            elif award_str in sent_str and i >= 2 and sent[i-1] == 'for' and sent[i-2] == 'winner':
                ca_set_awards += look_forward(sent, i, start='best')

    sorted_ca_winner = unique_ngrams(ca_set_winner)[:90]
    sorted_ca_winner = remove_duplicate_sublist(sorted_ca_winner)

    # remove all sublists at this point, re-rank according to occurrence frequency
    ca_winner = remove_all_sublists(sorted_ca_winner)

    ca_freq_winner = []
    for x in ca_awards:
        n = 0
        for tweet in gg2013:
            sent = tweet['text'].split()
            if is_Sublist(sent, x):
                n += 1
        ca_freq_winner.append([x, n])

    sorted_ca_freq_winners[i] = sorted(ca_freq_winner, key=lambda x: x[1], reverse=True)

