import numpy as np
from functools import reduce
import ray
import json
from copy import deepcopy


def look_forward(sent, ind, start=None, end=None, include=True, start_exclude=None, end_exclude=None, max_len=99):
    # speed up search by anchoring first to start
    if start is not None and not include and len(start) <= len(sent) - ind and sent[ind+1:ind+1+len(start)] == start:
        ind += len(start)
        start = None
    ngrams = []
    offset = None
    end_offset = 0
    if include:
        offset = 0
    elif start is not None and end is None:
        offset = len(start)
    elif end is not None and start is None:
        offset = len(end)
    elif start is None and end is None:
        offset = 0
    else:
        start_offset = len(start)
        end_offset = len(end)
        offset = start_offset
    for length in range(1, min(max_len + 1, len(sent) - ind)):
        if start_exclude is not None and sent[ind+1+offset] in start_exclude:
            break
        elif end_exclude is not None and sent[ind+length-end_offset] in end_exclude:
            break
        elif start is None and end is None:
            res = sent[ind+1: ind+length+1]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and end is None and len(start) <= length and sent[ind+1: ind+len(start)+1] == start:
            res = sent[ind+1+offset: ind+length+1]
            if len(res) > 0:
                ngrams.append(res)
        elif end is not None and start is None and len(end) <= length and sent[ind+length+1-len(end):ind+length+1] == end:
            res = sent[ind+1: ind+length+1-offset]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and end is not None and len(start) <= length and len(end) <= length and sent[ind+1: ind+len(start)+1] == start and sent[ind+length+1-len(end):ind+length+1] == end:
            res = sent[ind+1+start_offset: ind+length+1-end_offset]
            if len(res) > 0:
                ngrams.append(res)
                # greedy match end token
                break
    return ngrams


def look_backward(sent, ind, start=None, end=None, include=True, start_exclude=None, end_exclude=None, max_len=99):
    if end is not None and not include and len(end) <= len(sent) - ind and sent[ind-len(end):ind] == end:
        ind -= len(end)
        end = None
    ngrams = []
    offset = None
    start_offset = 0
    if include:
        offset = 0
    elif start is not None and end is None:
        offset = len(start)
    elif end is not None and start is None:
        offset = len(end)
    elif start is not None and end is not None:
        start_offset = len(start)
        end_offset = len(end)
        offset = end_offset
    else:
        offset = 0
    for length in range(1, min(max_len + 1, ind+1)):
        if end_exclude is not None and sent[ind - 1 - offset] in end_exclude:
            break
        elif start_exclude is not None and sent[ind - length + start_offset] in start_exclude:
            break
        elif start is None and end is None:
            res = sent[ind - length: ind]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and end is None and len(start) <= length and sent[ind-length: ind-length+len(start)] == start:
            res = sent[ind - length + offset: ind]
            if len(res) > 0:
                ngrams.append(res)
        elif end is not None and start is None and len(end) <= length and sent[ind-len(end):ind] == end:
            res = sent[ind - length: ind - offset]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and end is not None and len(start) <= length and len(end) <= length and sent[ind-length: ind-length+len(start)] == start and sent[ind-len(end):ind] == end:
            res = sent[ind - length + start_offset: ind - end_offset]
            if len(res) > 0:
                ngrams.append(res)
                # greedy match end token
                break
    return ngrams


# merge and count
def unique_ngrams(ngrams_lst):
    dict = {}
    cnt = []
    for ngram in ngrams_lst:
        ngram_str = ' '.join(ngram)
        if ngram_str not in dict:
            dict[ngram_str] = 1
        else:
            dict[ngram_str] += 1
    for ngram_str in dict:
        cnt.append([ngram_str.split(), dict[ngram_str]])
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


def filter_host_kwd(lst):
    found = False
    hosts = None
    kwds = ['the', 'golden globes', 'to', 'best', 'our', 'can', 'as']
    while not found:
        lst_str = ' '.join(lst[0][0])
        if lst_str in kwds:
            lst.remove(lst[0])
        else:
            hosts = lst[0][0]
            found = True
    return ' '.join(hosts).split(' and ')


def filter_award_sorted(lst):
    remove = []
    for i, award in enumerate(lst):
        if award[0][0] != 'best' or len(award[0]) <= 2:
            remove.append(i)
    remove = sorted(remove, reverse=True)
    for idx in remove:
        lst.remove(lst[idx])
    return lst


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


@ray.remote
def remove_duplicate_sublist_str_ts(sorted_ca):
    removal = []
    for ca1 in sorted_ca:
        for ca2 in sorted_ca:
            if len(ca1[0]) < len(ca2[0]) and ca1[0] in ca2[0] and ca1[1] <= ca2[1] * 1.5:
                if ca1 not in removal:
                    removal.append(ca1)
    for to_remove in removal:
        sorted_ca.remove(to_remove)
    return sorted_ca


@ray.remote
def remove_duplicate_sublist_str(sorted_ca):
    removal = []
    for ca1 in sorted_ca:
        for ca2 in sorted_ca:
            if is_StrictSublist(ca2[0].split(' '), ca1[0].split(' ')) and ca1[1] <= ca2[1]:
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


def remove_subsets_sorted(sorted_cand):
    sorted_cand = _remove_subsets_sorted(sorted_cand, limit=5)
    sorted_cand = _remove_subsets_sorted(sorted_cand, limit=10)
    return sorted_cand


def _remove_subsets_sorted(sorted_cand, limit=5, synonym=True):
    parents = [-np.inf] * len(sorted_cand)
    removal = []
    sorted_cand_rev = sorted_cand[::-1]
    all_sets = [set(c[0]) for c in sorted_cand_rev]
    for i, c_set in enumerate(all_sets):
        j = 0
        found = False
        while j < len(all_sets) and not found:
            if i == j:
                j += 1
                continue
            c_super = all_sets[j]
            if synonym and 'movie' in c_set:
                syn = set.union(c_set - {'movie'}, {'motion', 'picture'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            elif synonym and 'film' in c_set:
                syn = set.union(c_set - {'film'}, {'motion', 'picture'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and synonym and 'for' in c_set:
                syn = set.union(c_set - {'for'}, {'in', 'a'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and synonym and 'picture' in c_set and 'motion' not in c_set:
                syn = set.union(c_set - {'picture'}, {'motion', 'picture'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and synonym and 'television' in c_set:
                syn = set.union(c_set - {'television'}, {'tv'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and synonym and 'tv' in c_set and 'show' in c_set:
                syn = set.union(c_set - {'tv', 'show'}, {'tv', 'series'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and synonym and 'tv' in c_set and 'series' not in c_set:
                syn = set.union(c_set - {'tv'}, {'tv', 'series'})
                if syn <= c_super and len(' '.join(c_super - syn)) < limit:
                    removal.append(len(sorted_cand) - i - 1)
                    parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                    found = True
            if not found and ((c_set < c_super and len(' '.join(c_super - c_set)) < limit) or (c_set == c_super and i < j)):
                removal.append(len(sorted_cand) - i - 1)
                parents[len(sorted_cand) - i - 1] = len(sorted_cand) - j - 1
                found = True
            j += 1
    for idx in removal:
        score = sorted_cand[idx][1]
        # add candidate score to final parent node
        parent = parents[idx]
        while True:
            if parents[parent] != -np.inf:
                parent = parents[parent]
            else:
                break
        sorted_cand[parent][1] += score
    for idx in removal:
        sorted_cand.remove(sorted_cand[idx])
    return sorted(sorted_cand, key=lambda x: x[1], reverse=True)


# per award
@ray.remote
def untie_raw_winners(winners):
    return untie(unique_ngrams_ts(winners))


def untie(sorted_ca):
    if len(sorted_ca) == 0:
        return []
    freq = sorted_ca[0][1]
    max_len = 0
    best = None
    timestamp = None
    idx = 0
    while idx < len(sorted_ca) and sorted_ca[idx][1] == freq:
        if len(sorted_ca[idx][0]) > max_len:
            best = sorted_ca[idx][0]
            timestamp = sorted_ca[idx][2]
            max_len = len(sorted_ca[idx][0])
        idx += 1
    return [best, freq, timestamp]


@ray.remote
def unique_ngrams_ray(ngrams_lst, start=None):
    cnt = []
    all_ngrams = {}
    timestamp = {}
    timestamp_init = {}
    for ngrams in ngrams_lst:
        for ngram in ngrams[0]:
            ngram_str = ' '.join(ngram)
            if ngram_str not in all_ngrams:
                all_ngrams[ngram_str] = 1
                if start is None or ngrams[1] >= start:
                    timestamp[ngram_str] = ngrams[1]
                else:
                    timestamp[ngram_str] = np.inf
                    timestamp_init[ngram_str] = ngrams[1]
            # find earliest occurrence
            elif ngrams[1] < timestamp[ngram_str]:
                timestamp[ngram_str] = ngrams[1]
                all_ngrams[ngram_str] += 1
            else:
                all_ngrams[ngram_str] += 1
    # prefer occurrences after t=start
    for ng in timestamp.keys():
        if timestamp[ng] == np.inf:
            timestamp[ng] = timestamp_init[ng]
    for uni_ngram in all_ngrams:
        cnt.append([uni_ngram.split(), all_ngrams[uni_ngram], timestamp[uni_ngram]])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt


# merge and count
def unique_ngrams_ts(ngrams_lst, start=None):
    cnt = []
    all_ngrams = {}
    timestamp = {}
    timestamp_init = {}
    for ngrams in ngrams_lst:
        for ngram in ngrams[0]:
            ngram_str = ' '.join(ngram)
            if ngram_str not in all_ngrams:
                all_ngrams[ngram_str] = 1
                if start is None or ngrams[1] >= start:
                    timestamp[ngram_str] = ngrams[1]
                else:
                    timestamp[ngram_str] = np.inf
                    timestamp_init[ngram_str] = ngrams[1]
            # find earliest occurrence
            elif ngrams[1] < timestamp[ngram_str]:
                timestamp[ngram_str] = ngrams[1]
                all_ngrams[ngram_str] += 1
            else:
                all_ngrams[ngram_str] += 1
    # prefer occurrences after t=start
    for ng in timestamp.keys():
        if timestamp[ng] == np.inf:
            timestamp[ng] = timestamp_init[ng]
    for uni_ngram in all_ngrams:
        cnt.append([uni_ngram.split(), all_ngrams[uni_ngram], timestamp[uni_ngram]])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt


def unique_strs_ts(strs_lst, start=None):
    cnt = []
    all_strs = {}
    timestamp = {}
    timestamp_init = {}
    for strs in strs_lst:
        for ps in strs[0]:
            ps_str = ' & '.join(ps)
            if ps_str not in all_strs:
                all_strs[ps_str] = 1
                if start is None or strs[1] >= start:
                    timestamp[ps_str] = strs[1]
                else:
                    timestamp[ps_str] = np.inf
                    timestamp_init[ps_str] = strs[1]
            # find earliest occurrence
            elif strs[1] < timestamp[ps_str]:
                timestamp[ps_str] = strs[1]
                all_strs[ps_str] += 1
            else:
                all_strs[ps_str] += 1

    # prefer occurrences after t=start
    for pss in timestamp.keys():
        if timestamp[pss] == np.inf:
            timestamp[pss] = timestamp_init[pss]
    for uni_ngram in all_strs:
        # uni_ngram is list(str[, str])
        cnt.append([uni_ngram.split(' & '), all_strs[uni_ngram], timestamp[uni_ngram]])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt


@ray.remote
def filter_by_timestamp(candidates, indices, sorted_ts, relaxed):
    results = [[] for _ in range(len(sorted_ts))]
    for cand in candidates[indices[0]: indices[1]]:
        if cand[2] < sorted_ts[0]:
            continue
        i = 0
        while i < len(sorted_ts) and cand[2] > sorted_ts[i]:
            i += 1
        results[i - 1].append([cand[0], cand[1]])
        if relaxed:
            if i > 1:
                results[i - 2].append([cand[0], cand[1]])

    return results


@ray.remote
def disqualify_kwd(results):
    kwds = ['and', 'for', 'to', 'not', 'have', 'can', ' may', ' do', 'does', 'did', 'it', 'they', 'should', 'if',
            'but', 'that', 'im', 'shit', 'than', 'kinda', 'bc', 'though', 'kidding', 'ok', 'wtf', 'omg', 'def', 'boo',
            'ill', 'u', 'fuck', 'oscar', 'oscars', 'seriously', 'completely', 'who', 'sob', 'pretty', 'meh', 'totally',
            'nah', 'duh', 'cute', 'bummed', 'fav', 'ah', 'rt', 'waah', 'tweets', 'whos', 'pissed', 'lol', 'bye',
            'yeah', 'congrats', 'oh', 'peeved', 'sad', 'cant', 'except', 'surprised', 'upset', 'cry', 'crying',
            'explain', 'parlay', 'shouldve', 'methinks', 'think', 'glad', 'produced', 'produce', 'didnt', 'what', 'robbed',
            'shocked', 'ddl', 'she', 'he', 'they', 'really', 'my', 'faggot', 'you', 'hubby', 'well', 'shock', 'happier',
            'cannot', 'n', 'category', 'garbage', 'obviously', 'movies', 'yay', 'such', 'gotta', 'wait',
            'tonights', 'also', 'definitly', 'ew', 'just', 'or', 'tonight', 'bunch', 'since', 'sure',
            'performer', 'overrated', 'believe', 'actor', 'actress', 'ugh', 'woo', 'has', 't', 'nominee', 'nominees',
            'happy', 'suddenly', 'o', 'based', 'heck', 'gutted', 'stupid', 'usually' 'dumbass', 'bullshit', 'surprises',
            'there', 'those', 'theres', 'surely', 'damn', 'least', 'yeh', 'ton', 'tons', 'committed', 'gonna', 'come',
            'comes', 'came', 'snap', 'deff', 'gang', 'bb', 'mean', 'hair', 'dress', 'yes', 'awards', 'him', 'shouldnt',
            'tell', 'rigged', 'totes', 'probably', 'haha', 'suppose', 'because', 'song', 'why', 'when', 'win', 'aw',
            'film', 'bbt', 'no', 'yall', 'urgh', 'dang', 'welp', 'gf', 'fcking', 'tvd', 'favs', 'tig', 'apparently',
            'honestly', 'dam', 'ass', 'fucking', 'nope', 'motherfucker', 'goldenglobes', 'bae', 'dammit', 'netflix',
            'television', 'legit', 'sayin', 'snubbed', 'snub', 'barf', 'ahs', 'huh', 'oitnb', 'fave', 'hbo', 'boobs',
            'noo', 'freaking', 'soandso', 'agh', 'ointb', 'srsly', 'naw', 'disney', 'dag', 'salty', 'yep', 'dmx',
            'tl', 'woah', 'penis', 'fukk', 'awks', 'uzu', 'jld', 'tbh', 'bomer', 'desplat', 'richly', 'smh', 'cb', 'hating',
            'scene', 'd', 'globes', 's', 'gifs', 'gif', 'unfortunately', 'goddammit', 'soundtrack', 'nomination', 'news',
            'aargh', 'suprised', 'faves', 'actresses', 'actress', 'nominated', 'ya', 'disappointed', 'nawl', 'sia',
            'miniseries', 'knows']

    kwds_partial = ['sss', 'aaa', 'mmm', 'uuu', 'ooo', 'kkk', 'rrr', 'fff', 'ww', '_', 'truly believe', 'i think', 'suck',
                    'just won', 'pfft', 'ripped off', 'win something', "doesnt think", 'looks amazing', 'is amazing',
                    'i just', 'pleased with', 'golden globes', 'wow', 'at the', 'gah', 'just wait', 'i suppose',
                    'we just', 'at the', 'best performance', 'hh', 'gah', 'i mean', 'only reason', 'find out', 'nono',
                    'tv series', 'thank god', 'thank zeus', 'in the name', 'other news', 'behind on', 'nbc', 'right now',
                    'too bad', 'i know', 'its great', 'dear god', 'best picture', 'looks like', 'i feel', 'best screenplay',
                    'of them', 'would know', 'globes then', 'golden globe', 'i saw', 'the cw', 'be honest', 'the hell',
                    'i call', 'we all', 'once again', 'the good', 'so sorry', 'them all', 'too soon', 'leave after',
                    'all the other', 'was awesome', 'found out', 'her friend', 'every person', 'join me', 'everybody knows',
                    'remind us', 'was terrible', 'feel comfortable', 'call the police', 'i thought', 'as long as',
                    'always felt', 'i guess', 'this show', 'like the', 'like i', 'feel like', 'ive heard', 'this day',
                    'your favorites', 'i love', 'all in all', 'is theft', 'all the way', 'globe award', 'a lot', 'proud of',
                    'was incredible', 'hate this', 'a tuesday', 'the cast', 'guess i', 'wins the', 'first off', 'me after',
                    'all know', 'heavy hitters', 'understand how', 'use these', 'weve all', 'all the', 'go all',
                    'how the']

    kwds_full = ['the', 'me', 'follow', 'new', 'goldenglobes', 'guy', 'at', 'from', 'in', 'sound', 'so', 'girl',
                 'too', 'her', 'his', 'les', 'lee', 'amy', 'tina', 'say', 'even', 'dick', 'further', 'god',
                 'like', 'agree', 'listen', 'truly', 'wasnt', 'ga', 'where', 'ub', 'with the', 'actually', 'beautiful',
                 'actors', 'picks', 'still', 'use', 'about', 'was the', 'definitely', 'sigh', 'maybe', 'choice',
                 'which', 'ummm', 'mad', 'hoc', 'this is', 'we', 'got', 'everyone', 'so gett', 'give', 'way', 'more',
                 'globes', 'on', 'watch', 'score', 'imo', 'game', 'channel', 'gone', 'list', 'perfect', 'coming',
                 'out', 'her friend', 'two', 'cool', 'move', 'off', 'common', 'lord', 'hell', 'queen', 'awesome',
                 'disappointed', 'flip over', 'wrong', 'someone', 'sweet', 'tomorrow', 'cher', 'switch', 'hoc', 'mention',
                 'now', 'dam', 'sorry', 'nominated', 'mm', 'by', 'some', 'nor', 'either', 'them', 'again', 'submit',
                 'absolutely', 'else', 'anyone', 'this', 'on', 'watch', 'hear', 'award', 'both', 'hadnt', 'sadly',
                 'settle', 'green', 'cher', 'weather', 'avaetc', 'showperson', 'possible', 'certainly', 'picture',
                 'choices', 'cashew', 'arguably', 'person', 'boy', 'screenplay', 'go', 'up', 'call', 'baby', 'imagine',
                 'felt', 'spring', 'rides', 'bull', 'globe', 'the best', 'best', 'artist', 'soon', 'series', 'good',
                 'flip', 'saw', 'mom', 'dragons', 'downton', 'terrible', 'the show', 'lana', 'honest', 'day',
                 'show', 'crazy', 'character', 'cast', 'incredible', 'know', 'viola', 'tuesday', 'golden', 'true', '2 of',
                 '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    res = []
    for e in results:
        e_str = ' '.join(e[0])
        found = False
        i = 0
        kwds_len = len(kwds)
        kwds_par_len = len(kwds_partial)
        kwds_ful_len = len(kwds_full)
        while not found and i < kwds_len:
            if kwds[i] in e[0]:
                found = True
            i += 1
        i = 0
        while not found and i < kwds_par_len:
            if e_str.find(kwds_partial[i]) != -1:
                found = True
            i += 1
        i = 0
        while not found and i < kwds_ful_len:
            if kwds_full[i] == e_str:
                found = True
            i += 1
        if not found:
            res.append([e_str, e[1]])
    return res


@ray.remote
def disqualify_kwd_str(results, kwds_full):
    kwds = ['and', 'for', 'to', 'not', 'have', 'can', ' may', ' do', 'does', 'did', 'it', 'they', 'should', 'if',
            'but', 'that', 'im', 'shit', 'than', 'kinda', 'bc', 'though', 'kidding', 'ok', 'wtf', 'omg', 'def', 'boo',
            'ill', 'u', 'fuck', 'oscar', 'oscars', 'seriously', 'completely', 'who', 'sob', 'pretty', 'meh', 'totally',
            'nah', 'duh', 'cute', 'bummed', 'fav', 'ah', 'rt', 'waah', 'tweets', 'whos', 'pissed', 'lol', 'bye',
            'yeah', 'congrats', 'oh', 'peeved', 'sad', 'cant', 'except', 'surprised', 'upset', 'cry', 'crying',
            'explain', 'parlay', 'shouldve', 'methinks', 'think', 'glad', 'produced', 'produce', 'didnt', 'what', 'robbed',
            'shocked', 'ddl', 'she', 'he', 'they', 'really', 'my', 'faggot', 'you', 'hubby', 'well', 'shock', 'happier',
            'cannot', 'n', 'category', 'garbage', 'obviously', 'movie', 'movies', 'yay', 'such', 'gotta', 'wait',
            'tonights', 'also', 'definitly', 'ew', 'just', 'or', 'tonights', 'tonight', 'bunch', 'since', 'sure',
            'performer', 'overrated', 'believe', 'actor', 'actress', 'ugh', 'woo', 'has', 't', 'o', 'nominee', 'nominees',
            'happy', 'suddenly', 'based', 'heck', 'gutted', 'stupid', 'usually' 'dumbass', 'bullshit', 'surprises',
            'there', 'those', 'theres', 'surely', 'damn', 'least', 'yeh', 'ton', 'tons', 'committed', 'gonna', 'come',
            'comes', 'came', 'snap', 'deff', 'gang', 'bb', 'mean', 'hair', 'dress', 'yes', 'awards', 'him', 'shouldnt',
            'tell', 'rigged', 'totes', 'probably', 'haha', 'suppose', 'because', 'song', 'why', 'when', 'win', 'aw',
            'film', 'bbt', 'no', 'the', 'me', 'follow', 'new', 'goldenglobes', 'guy', 'at', 'from', 'in', 'sound', 'so',
            'girl', 'too', 'her', 'his', 'les', 'say', 'even', 'dick', 'further', 'god', 'i', 'golden', 'globes', 'by',
            'pics', 'may', 'isnt', 'funny', 'book', 'ever', 'had', 'has', 'have', 'off', 'looking', 'of', 'like', 'video',
            'and', 'needs', 'need', 'you', 'would', 'the', 'on', 'lol', 'was', 'how', 'finally', 'a', 'are', 'is', 'be',
            'must', 'hilarious', 'as', 'their', 'up', 'during', 'back', 'big', 'which', 'having', 'makes', 'make',
            'these', 'see', 'been', 'photoset', 'photo', 'our', 'while', 'where', 'more', 'present', 'presenter',
            'presenters', 'presents', 'already', 'its', 'onstage', 'fans', 'now', 'got', 'all', 'seem', 'with', 'loved',
            'saw', 'way', 'old', 'wow', 'many', 'want', 'convince', 'ask', 'faceburn', 'carpet', 'including', 'great',
            'live', 'stay', 'watched', 'one', 'out', 'us', 'mini', 'maybe', 'women', 'could', 'greats', 'planned',
            'barely', 'couldnt', 'theyre', 'horrible', 'description', 'youre', 'gift', 'beyond', 'become', 'allowed',
            'same', 'everyone', 'else', 'standing', 'were', 'amazing', 'se', 'photos', 'gorgeous', 'captcha', 'heres',
            'love', 'definitely', 'night', 'this', 'winning', 'man', 'men', 'hollywood', 'told', 'costars', 'speak',
            'notice', 'boss', 'en', 'y', 'b', 'q', 'si', 'un', 'che', 'el', 'sexy', 'que', 'unidos', 'favorito', 'gente',
            'arrived', 'la', 'le', 'together', 'watching', 'nos', 'cuando', 'estan', 'tres', 'beau', 'hola', 'actors',
            'actor', 'shes', 'hes', 'done', 'terracolombia', 'miss', 'con', 'actriz', 'guaperrima', 'mr', 'hermosa',
            'woman', 'aux', 'ransom', 'hon', 'let', 'watch', 'verdadero', 'common', 'loving', 'love', 'seeing', 'jokes',
            'wife', 'christ', 'talkin', 'always', 'turn', 'your', 'looks', 'seen', 'wifes', 'says', 'press', 'saying',
            'award', 'goes', 'moment', 'wearing', 'fun', 'alums', 'baby', 'glasses', 'awkward', 'most', 'skipped',
            'television', 'picture', 'every', 'course', 'nbc', 'best', 'known', 'go', 'about', 'missed', 'before',
            'after', 'fucking', 'gives', 'rather', 'very', 'sm', 'thus', 'lovely']

    kwds_partial = ['president', 'yaa', 'hah', 'www', 'hmm', 'ooo', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                    'official', 'tweet']

    res = []
    for e in results:
        # for p1 and p2 in e
        found = False
        for p in e[0]:
            if p in kwds_full: found = True; break
            i = 0
            kwds_par_len = len(kwds_partial)
            for x in p.split(' '):
                if x in kwds:
                    found = True; break
            while not found and i < kwds_par_len:
                if kwds_partial[i] in p:
                    found = True
                i += 1
        if not found:
            res.append(e)
    return res


@ray.remote
def remove_dup_single_ts(res):
    remove = []
    for i, e in enumerate(res):
        e_l = e[0].split(' ')
        if len(e_l) == 1:
            # comparing against top 15 works better on gg2013
            for j, x in enumerate(res):
                x_l = x[0].split(' ')
                if len(x_l) > 1 and e[0] in x_l:
                    if e[1] < x[1] / 0.3:
                        remove.append(i)
    remove = sorted(remove, reverse=True)
    for idx in remove:
        res.remove(res[idx])
    return res


@ray.remote
def remove_dup_single(res):
    top = deepcopy(res[:15])
    for e in res:
        e_l = e[0].split(' ')
        cand_sups = []
        for x in top:
            if len(x[0].split(' ')) > 1:
                cand_sups.append(x[0])
        if len(e_l) == 1 and any([e[0] in c for c in cand_sups]):
            res.remove(e)
    return res


# names presumed to have two or three words
def find_presenters(sent, backward=True, eng=True):
    if eng:
        dlmtr = 'and'
    else:
        dlmtr = 'y'
    presenters = []
    if backward:
        if len(sent) > 2 and sent[-3] == dlmtr:
            p1 = ' '.join(sent[-2:])
            if len(sent) > 4:
                presenters.append([' '.join(sent[-5:-3]), p1])
            if len(sent) > 5:
                presenters.append([' '.join(sent[-6:-3]), p1])
        elif len(sent) > 3 and sent[-4] == dlmtr:
            p1 = ' '.join(sent[-3:])
            if len(sent) > 5:
                presenters.append([' '.join(sent[-6:-4]), p1])
            if len(sent) > 6:
                presenters.append([' '.join(sent[-7:-4]), p1])
        else:
            if len(sent) > 1:
                presenters.append([' '.join(sent[-2:])])
            if len(sent) > 2:
                presenters.append([' '.join(sent[-3:])])
    else:
        if len(sent) > 2 and sent[2] == dlmtr:
            p1 = ' '.join(sent[:2])
            if len(sent) > 4:
                presenters.append([' '.join(sent[3:4]), p1])
            if len(sent) > 5:
                presenters.append([' '.join(sent[3:5]), p1])
        elif len(sent) > 3 and sent[3] == dlmtr:
            p1 = ' '.join(sent[:3])
            if len(sent) > 5:
                presenters.append([' '.join(sent[4:6]), p1])
            if len(sent) > 6:
                presenters.append([' '.join(sent[4:7]), p1])
        else:
            if len(sent) > 1:
                presenters.append([' '.join(sent[:2])])
            if len(sent) > 2:
                presenters.append([' '.join(sent[:3])])
    return presenters


@ray.remote
def combine_presenters(lst):
    remove = []
    for i, l in enumerate(lst):
        for j, m in enumerate(lst):
            if j == i or i in remove or j in remove:
                continue
            elif set(l[0]) == set(m[0]):
                remove.append(j)
                l[1] += m[1]
            elif len(l[0]) == 1 and set(l[0]) < set(m[0]):
                remove.append(i)
                m[1] = max(m[1], l[1])
            elif len(m[0]) == 1 and set(m[0]) < set(l[0]):
                remove.append(j)
                l[1] = max(m[1], l[1])

    remove = sorted(remove, reverse=True)
    for idx in remove:
        lst.remove(lst[idx])
    # prefer pairs by summing counts for individuals in pairs
    return sorted(lst, key=lambda x:x[1], reverse=True)


@ray.remote
def combine_presenter_sublists(lst):
    for i, l in enumerate(lst):
        for j, m in enumerate(lst):
            if j == i:
                continue
            l_match = True
            hard_combine = True
            for ll in l[0]:
                m_match = False
                for mm in m[0]:
                    if ll in mm:
                        m_match = True
                        if len(ll.split()) != len(mm.split()):
                            hard_combine = False
                if not m_match:
                    l_match = False
            if l_match and hard_combine:
                lst[j][1] += lst[i][1]
            elif l_match and l[1] * 0.75 < m[1]:
                lst[j][1] /= 0.75
    # prefer pairs by summing counts for individuals in pairs
    return sorted(lst, key=lambda x:x[1], reverse=True)


@ray.remote
def rerank(data, indices, lsts):
    # zero counts
    freq_lsts = [[l, 0] for l in lsts]
    texts = data.iloc[range(*indices)]['text'].tolist()
    for text in texts:
        for l in freq_lsts:
            if ' '.join(l[0]) in text:
                l[1] += 1
    # lsts = sorted(lsts, key=lambda x: x[1], reverse=True)
    return freq_lsts


# return string names for awards
def combine_sort(results):
    return sorted(reduce(lambda x, y: list(map(lambda a, b: [a[0], a[1] + b[1]], x, y)), results), key=lambda x: x[1], reverse=True)


@ray.remote
def rerank_nominees(data, indices, lsts):
    # re-rank based on occurrence frequency
    for l in lsts:
        for e in l: e[1] = 0
    texts = data.iloc[range(*indices)]['text'].tolist()
    for text in texts:
        for i, g in enumerate(lsts):
            for x in g:
                if x[0] in text:
                    x[1] += 1
    # candidates = [sorted(lsts[i], key=lambda x: x[1], reverse=True) for i in range(len(lsts))]
    return lsts


@ray.remote
def rerank_ts(data, indices, lsts, ts):
    # zero counts
    ts_len = len(ts)
    names_lst = []
    for i, lst in enumerate(lsts):
        names, _ = zip(*lst)
        names_lst.append(names)
    for l in lsts:
        for e in l: e[1] = 0
    texts = data.iloc[range(*indices)]['text'].tolist()
    tss = data.iloc[range(*indices)]['timestamp_ms'].tolist()
    for n, text in enumerate(texts):
        if 'dress' not in text or 'present' in text:
            t = tss[n]
            i = 0
            while i < ts_len and t > ts[i]:
                i += 1
            i -= 1
            if i > -1:
                for j, x in enumerate(names_lst[i]):
                    for k, y in enumerate(x):
                        if y in text:
                            lsts[i][j][1] += 1
    '''# average counts for pairs
    for i, lst in enumerate(new_lsts):
        for j, x in enumerate(lst):
            new_lsts[i][j][1] /= len(new_lsts[i][j][0])'''
    return lsts


def ray_data_workers(func_ray, func_comb, n_CPU, data_len, data_ref, *params):
    results = []
    result_refs = []
    for cpu in range(n_CPU):
        result_refs.append(func_ray.remote(data_ref, [int(data_len / n_CPU * cpu), int(data_len / n_CPU * (cpu + 1))], *params))
    for ref in result_refs:
        results.append(ray.get(ref))
    return func_comb(results)


def ray_res_workers(func_ray, func_comb, data, *params):
    results = []
    results_refs = []
    for d in data:
        results_refs.append(func_ray.remote(d, *params))
    for ref in results_refs:
        results.append(ray.get(ref))
    return func_comb(results)


def rerank_combine(results):
    combined = zip(*results)
    merged = [[] for _ in range(len(results[0]))]
    for i, lsts in enumerate(combined):
        keys = [x[0] for x in lsts[0]]
        for k in keys:
            score = sum([x[1] if x[0] == k else 0 for e in lsts for x in e])
            merged[i].append([k, score])
    return [sorted(lst, key=lambda x:x[1], reverse=True) for lst in merged]


def collect(results):
    return results


def collect_combine(results):
    return reduce(lambda a, b: a+b, results)


def collect_combine_n(results):
    return reduce(lambda a, b: list(map(lambda x, y: x+y, a, b)), results)


def capitalize(str_lst):
    for i, string in enumerate(str_lst):
        strs = string.split(' ')
        for j, str in enumerate(strs):
            if j == 0 or str not in ['and', 'or', 'in', 'a', 'an', 'by', 'to', 'of', 'for']:
                char = str[0]
                strs[j] = char.upper() + str[1:]
        str_lst[i] = ' '.join(strs)
    return str_lst


# debugging only
def eval_nominees(nominees, year, award_map_inv, awards):
    ans = json.load(open(f'gg{year}answers.json'))
    p = [ans['award_data'][award_map_inv[' '.join(a)]]['nominees'] for a in awards]
    p_res = [[] for _ in range(len(p))]
    true = tot = 0
    for i, ps in enumerate(p):
        for n in ps:
            # exact spelling required
            if n in nominees[i]:
                p_res[i].append([n, True]); true += 1; tot += 1
            else:
                p_res[i].append([n, False]); tot += 1
    print(f'Nominees extraction accuracy is {true / tot:.2%}')
    # print(f'Full: {true}/{tot}')
    for i, r in enumerate(p_res):
        print(r)
        # print(nominees[i])


# debugging only
def eval_presenters(presenters, year, award_map_inv, awards):
    ans = json.load(open(f'gg{year}answers.json'))
    p = [ans['award_data'][award_map_inv[' '.join(a)]]['presenters'] for a in awards]
    p_res = [[] for _ in range(len(p))]
    true = tot = 0
    for i, ps in enumerate(p):
        for n in ps:
            # exact spelling required
            if n in presenters[i]:
                p_res[i].append([n, True]); true += 1; tot += 1
            else:
                p_res[i].append([n, False]); tot += 1
    print(f'Presenters extraction accuracy is {true / tot:.2%}')
    # print(f'Full: {true}/{tot}')
    for i, r in enumerate(p_res):
        print(r)
        # print(presenters[i])
