from data import *
import numpy as np
import pandas as pd


def look_forward(sent, ind, start=None, end=None):
    ngrams = []
    for length in range(1, len(sent) - ind):
        if start is None and end is None:
            ngrams.append(sent[ind+1: ind+length+1])
        elif start is not None and len(start) <= length and sent[ind+1: ind+len(start)+1] == start:
            ngrams.append(sent[ind+1: ind+length+1])
        elif end is not None and len(end) <= length and sent[ind+length+1-len(end):ind+length+1] == end:
            ngrams.append(sent[ind+1: ind+length+1])
    return ngrams


def look_backward(sent, ind, start=None, end=None):
    ngrams = []
    for length in range(1, ind+1):
        if start is None and end is None:
            ngrams.append(sent[ind-length: ind])
        elif start is not None and len(start) <= length and sent[ind-length: ind-length+len(start)] == start:
            ngrams.append(sent[ind - length: ind])
        elif end is not None and len(end) <= length and sent[ind-len(end):ind] == end:
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


def find_inter(ngrams_lsts):
    inter_cnt = []
    sorted_cnts = []
    for lst in ngrams_lsts:
        sorted_cnts.extend(unique_ngrams(lst))
    cand, freq = zip(*sorted_cnts)
    cand = np.asarray([' '.join(c) for c in cand])
    uniq_cand = list(set(cand))
    for c in uniq_cand:
        idxs = np.argwhere(cand == c)
        if len(idxs) >= len(ngrams_lsts):
            cnt = 0
            for idx in idxs:
                cnt += freq[idx.item()]
            inter_cnt.append([c.split(' '), cnt])
    return inter_cnt


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


def untie(sorted_ca):
    if len(sorted_ca) == 0:
        return []
    freq = sorted_ca[0][1]
    max_len = 0
    best = None
    idx = 0
    while idx < len(sorted_ca) and sorted_ca[idx][1] == freq:
        if len(sorted_ca[idx][0]) > max_len:
            best = sorted_ca[idx][0]
            max_len = len(sorted_ca[idx][0])
        idx += 1
    return [best, freq]


'''# extract host, equal weights for all methods
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
            ca_set_awards += look_backward(sent, i, start=['best'])
        # xxx won best xxx
        elif word == 'won' and i > 0 and sent[i-1] == 'has':
            ca_set_awards += look_forward(sent, i-1, start=['best'])
        elif word == 'won':
            ca_set_awards += look_forward(sent, i, start=['best'])
        # best xxx - *winner
        # elif word == '-':
        #     ca_set_awards += look_backward(sent, i, start=['best'])

sorted_ca_awards = unique_ngrams(ca_set_awards)[:90]
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

sorted_ca_freq_awards = sorted(ca_freq_awards, key=lambda x :x[1], reverse=True)'''

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



# xxx (has) won AWARD
# xxx has won the golden globe for AWARD
# xxx ... for her golden globe win as AWARD


n_awards = len(awards)
sorted_ca_freq_winners = [None] * n_awards
sorted_ca_freq_presenters = [None] * n_awards
sorted_ca_freq_nominees = [None] * n_awards

for i, award in enumerate(awards):
    # get award names
    award_str = ' '.join(award)
    ca_set_winner = []
    ca_set_presenter = []
    for tweet in gg2013:
        sent = tweet['text'].split()
        for j, word in enumerate(sent):
            sent_str = ' '.join(sent[j: j + len(award)])
            if award_str == sent_str:
                # AWARD goes to xxx
                if j + len(award) + 2 < len(sent) and sent[j + len(award)] == 'goes' and sent[j + len(award) + 1] == 'to':
                    ca_set_winner += look_forward(sent, j + len(award) - 1, start=['goes', 'to'])[2:]
                # AWARD is awarded to xxx
                elif j + len(award) + 3 < len(sent) and sent[j + len(award)] == 'is' and sent[j + len(award) + 1] == 'awarded' and sent[j + len(award) + 2] == 'to':
                    ca_set_winner += look_forward(sent, j + len(award) - 1, start=['is', 'awarded', 'to'])[3:]
                # AWARD: xxx
                elif j + len(award) < len(sent):
                    ca_set_winner += look_forward(sent, j + len(award) - 1)
                # winner for AWARD is xxx
                elif j >= 2 and sent[j - 1] == 'for' and sent[j - 2] == 'winner' and len(sent) > j + len(award) + 1 and sent[j + len(award)] == 'is':
                    ca_set_winner += look_forward(sent, j + len(award) - 1, start=['is'])[1:]
                # AWARD is xxx
                elif len(sent) > j + len(award) and sent[j + len(award)] == 'is':
                    ca_set_winner += look_forward(sent, j + len(award) - 1, start=['is'])[1:]
                # someone presents AWARD to xxx
                elif j >= 2 and len(sent) > j + len(award) and sent[j + len(award)] == 'to' and sent[j - 1] == 'presents':
                    ca_set_winner += look_forward(sent, j + len(award) - 1, start=['to'])[1:]
                # xxx for AWARD
                elif j >= 2 and sent[j - 1] == 'for':
                    ca_set_winner += look_backward(sent, j, end=['for'])[:-1]
                # xxx wins/won/has won AWARD
                elif j >= 2 and sent[j - 1] == 'wins':
                    ca_set_winner += look_backward(sent, j, end=['wins'])[:-1]
                elif j >= 2 and sent[j - 1] == 'won':
                    ca_set_winner += look_backward(sent, j, end=['won'])[:-1]
                elif j >= 3 and sent[j - 1] == 'won' and sent[j - 2] == 'has':
                    ca_set_winner += look_backward(sent, j, end=['has', 'won'])[:-2]
                # xxx wins/won the #goldenglobe for AWARD
                elif j >= 4 and sent[j - 1] == 'for' and sent[j - 2] == 'goldenglobe' and sent[j - 3] == 'the' and sent[j - 4] == 'wins':
                    ca_set_winner += look_backward(sent, j, end=['wins', 'the', 'goldenglobe', 'for'])[:-4]
                elif j >= 4 and sent[j - 1] == 'for' and sent[j - 2] == 'goldenglobe' and sent[j - 3] == 'the' and sent[j - 4] == 'won':
                    ca_set_winner += look_backward(sent, j, end=['won', 'the', 'goldenglobe', 'for'])[:-4]
                # xxx - #goldenglobe winner for AWARD
                elif j >= 2 and sent[j - 1] == 'for' and sent[j - 2] == 'winner':
                    ca_set_winner += look_backward(sent, j, end=['goldenglobe', 'winner', 'for'])[:-3]
                # congrat/congrats to xxx for her/his golden globe win as AWARD
                elif j > 6 and sent[j - 1] == 'as' and sent[j - 2] == 'win' and sent[j - 3] == 'globe' and sent[j - 4] == 'golden' and sent[j - 5] == 'his' and sent[j - 6] == 'for':
                    ca_set_winner += look_backward(sent, j, end=['for', 'his', 'golden', 'globe', 'win', 'as'])[:-6]
                elif j > 6 and sent[j - 1] == 'as' and sent[j - 2] == 'win' and sent[j - 3] == 'globe' and sent[j - 4] == 'golden' and sent[j - 5] == 'her' and sent[j - 6] == 'for':
                    ca_set_winner += look_backward(sent, j, end=['for', 'her', 'golden', 'globe', 'win', 'as'])[:-6]

                # xxxx is presenting AWARD
                if j >= 2 and sent[j - 1] == 'presenting' and sent[j - 2] == 'is':
                    ca_set_presenter += look_backward(sent, j, end=['is', 'presenting'])[:-2]
                # someone presents AWARD to xxx
                elif j >= 1 and (sent[j - 1] == 'presents' or sent[j - 1] == 'presented'):
                    ca_set_presenter += look_backward(sent, j, end=['presents'])[:-1]
                # someone presents for AWARD
                elif j >= 2 and sent[j - 2] == 'presents' and sent[j - 1] == 'for':
                    ca_set_presenter += look_backward(sent, j, end=['presents', 'for'])[:-2]

    '''# take intersection of all
    while [] in ca_set_winner:
        ca_set_winner.remove([])
    sorted_ca_winner = find_inter(ca_set_winner)[:10]'''
    sorted_ca_freq_winners[i] = untie(unique_ngrams(ca_set_winner))
    sorted_ca_freq_presenters[i] = untie(unique_ngrams(ca_set_presenter))

    '''ca_freq_winner = []
    for x, _ in sorted_ca_winner:
        n = 0
        for tweet in gg2013:
            sent = tweet['text'].split()
            if is_Sublist(sent, x):
                n += 1
        ca_freq_winner.append([x, n])

    sorted_ca_freq_winners[i] = sorted(ca_freq_winner, key=lambda x: x[1], reverse=True)'''

print()

# find nominees for each award

