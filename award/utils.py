import numpy as np
from copy import deepcopy
from line_profiler_pycharm import profile


@profile
def look_forward(sent, ind, start=None, end=None, include=True, start_exclude=None):
    ngrams = []
    offset = None
    if include:
        offset = 0
    elif start is not None:
        offset = len(start)
    elif end is not None:
        offset = len(end)
    for length in range(1, len(sent) - ind):
        if start_exclude is not None and sent[ind+1] in start_exclude:
            break
        elif start is None and end is None:
            res = sent[ind+1: ind+length+1]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and len(start) <= length and sent[ind+1: ind+len(start)+1] == start:
            res = sent[ind+1+offset: ind+length+1]
            if len(res) > 0:
                ngrams.append(res)
        elif end is not None and len(end) <= length and sent[ind+length+1-len(end):ind+length+1] == end:
            res = sent[ind+1: ind+length+1-offset]
            if len(res) > 0:
                ngrams.append(res)
    return ngrams


def look_backward(sent, ind, start=None, end=None, include=True, end_exclude=None):
    ngrams = []
    offset = None
    if include:
        offset = 0
    elif start is not None:
        offset = len(start)
    elif end is not None:
        offset = len(end)
    for length in range(1, ind+1):
        if end_exclude is not None and sent[ind - 1] in end_exclude:
            break
        elif start is None and end is None:
            res = sent[ind - length: ind]
            if len(res) > 0:
                ngrams.append(res)
        elif start is not None and len(start) <= length and sent[ind-length: ind-length+len(start)] == start:
            res = sent[ind - length + offset: ind]
            if len(res) > 0:
                ngrams.append(res)
        elif end is not None and len(end) <= length and sent[ind-len(end):ind] == end:
            res = sent[ind - length: ind - offset]
            if len(res) > 0:
                ngrams.append(res)
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


# removes in-place
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


# merge and count
def unique_ngrams_ts(ngrams_lst, start=None):
    uni = []
    cnt = []
    all_ngrams = []
    timestamp = {}
    timestamp_init = {}
    for ngrams in ngrams_lst:
        all_ngrams.extend(ngrams[0])
        for ngram in ngrams[0]:
            ngram_str = ' '.join(ngram)
            if ngram not in uni:
                uni.append(ngram)
                if start is None or ngrams[1] >= start:
                    timestamp[ngram_str] = ngrams[1]
                else:
                    timestamp[ngram_str] = np.inf
                    timestamp_init[ngram_str] = ngrams[1]
            # find earliest occurrence
            elif ngrams[1] < timestamp[ngram_str]:
                timestamp[ngram_str] = ngrams[1]
    # prefer occurrences after t=start
    for ng in timestamp.keys():
        if timestamp[ng] == np.inf:
            timestamp[ng] = timestamp_init[ng]
    for uni_ngram in uni:
        cnt.append([uni_ngram, all_ngrams.count(uni_ngram), timestamp[' '.join(uni_ngram)]])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt


def unique_strs_ts(strs_lst, start=None):
    uni = []
    cnt = []
    all_strs = []
    timestamp = {}
    timestamp_init = {}
    for strs in strs_lst:
        all_strs.extend(strs[0])
        for ps in strs[0]:
            ps_str = ' & '.join(ps)
            if ps not in uni:
                uni.append(ps)
                if start is None or strs[1] >= start:
                    timestamp[ps_str] = strs[1]
                else:
                    timestamp[ps_str] = np.inf
                    timestamp_init[ps_str] = strs[1]
            # find earliest occurrence
            elif strs[1] < timestamp[ps_str]:
                timestamp[ps_str] = strs[1]
    # prefer occurrences after t=start
    for pss in timestamp.keys():
        if timestamp[pss] == np.inf:
            timestamp[pss] = timestamp_init[pss]
    for uni_ngram in uni:
        # uni_ngram is list(str[, str])
        cnt.append([uni_ngram, all_strs.count(uni_ngram), timestamp[' & '.join(uni_ngram)]])
    sorted_cnt = sorted(cnt, key=lambda x: x[1], reverse=True)
    return sorted_cnt


def filter_by_timestamp(candidates, sorted_ts, relaxed=False):
    results = [[] for _ in range(len(sorted_ts))]
    for cand in candidates:
        if cand[2] < sorted_ts[0]:
            '''for j in range(len(sorted_ts)):
                results[j].append([cand[0], cand[1]])'''
            continue
        i = 0
        while i < len(sorted_ts) and cand[2] > sorted_ts[i]:
            i += 1
        results[i - 1].append([cand[0], cand[1]])
        if relaxed:
            '''if i < len(results):
                results[i].append([cand[0], cand[1]])'''
            if i > 1:
                results[i - 2].append([cand[0], cand[1]])
            '''if i > 2:
                results[i - 3].append([cand[0], cand[1]])
            if i > 3:
                results[i - 4].append([cand[0], cand[1]])
            if i > 4:
                results[i - 5].append([cand[0], cand[1]])
            if i > 5:
                results[i - 6].append([cand[0], cand[1]])
            if i > 6:
                results[i - 7].append([cand[0], cand[1]])'''
    return results


def disqualify_kwd(results):
    kwds = ['and', 'for', 'to', 'not', 'have', 'can', ' may', ' do', 'does', 'did', 'it', 'they', 'should', 'if',
            'but', 'that', 'im', 'shit', 'than', 'kinda', 'bc', 'though', 'kidding', 'ok', 'wtf', 'omg', 'def', 'boo',
            'ill', 'u', 'fuck', 'oscar', 'oscars', 'seriously', 'completely', 'who', 'sob', 'pretty', 'meh', 'totally',
            'nah', 'duh', 'cute', 'bummed', 'fav', 'ah', 'rt', 'waah', 'tweets', 'whos', 'pissed', 'lol', 'bye',
            'yeah', 'congrats', 'oh', 'peeved', 'sad', 'cant', 'except', 'surprised', 'upset', 'cry', 'crying',
            'explain', 'parlay', 'shouldve', 'methinks', 'think', 'glad', 'produced', 'produce', 'didnt', 'what', 'robbed',
            'shocked', 'ddl', 'she', 'he', 'they', 'really', 'my', 'faggot', 'you', 'hubby', 'well', 'shock', 'happier',
            'cannot', 'n', 'category', 'garbage', 'obviously', 'movie', 'movies', 'yay', 'such', 'gotta', 'wait',
            'tonights', 'also', 'definitly', 'ew', 'just', 'or', 'tonights', 'tonight', 'bunch', 'since', 'sure',
            'performer', 'overrated', 'believe', 'actor', 'actress', 'ugh', 'woo', 'has', 't', 'nominee', 'nominees',
            'happy', 'suddenly', 'o', 'based', 'heck', 'gutted', 'stupid', 'usually' 'dumbass', 'bullshit', 'surprises',
            'there', 'those', 'theres', 'surely', 'damn', 'least', 'yeh', 'ton', 'tons', 'committed', 'gonna', 'come',
            'comes', 'came', 'snap', 'deff', 'gang', 'bb', 'mean', 'hair', 'dress', 'yes', 'awards', 'him', 'shouldnt',
            'tell', 'rigged', 'totes', 'probably', 'haha', 'suppose', 'because', 'song', 'why', 'when', 'win', 'aw',
            'film', 'bbt', 'no']
    kwds_partial = ['sss', 'ooo', 'kkk', 'rrr', 'fff', 'ww', '_', 'truly believe', 'i think', 'suck',
                    'just won', 'pfft', 'ripped off', 'win something', "doesnt think", 'looks amazing', 'is amazing'
                    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                    'i just', 'pleased with', 'golden globes', 'wow', 'at the', 'gah', 'just wait', 'i suppose',
                    'we just', 'at the', 'best performance', 'hh', 'gah', 'i mean', 'only reason', 'find out', 'nono',
                    'tv series', 'thank god', 'thank zeus', 'in the name', 'other news', 'behind on', 'nbc', 'right now',
                    'too bad', 'i know', 'its great', 'dear god']
    kwds_full = ['the', 'me', 'follow', 'new', 'goldenglobes', 'guy', 'at', 'from', 'in', 'sound', 'so', 'girl',
                 'too', 'her', 'his', 'les', 'lee', 'amy', 'tina', 'say', 'even', 'dick', 'further', 'god']
    res = []
    for g in results:
        g_res = []
        for e in g:
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
                g_res.append([e_str, e[1]])
        res.append(g_res)
    return res


def disqualify_kwd_str(results):
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
            'woman', 'aux', 'ransom', 'hon', 'let', 'watch', 'verdadero']
    kwds_partial = ['president', 'yaa', 'hah', 'hmm', 'ooo', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

    res = []
    for g in results:
        g_res = []
        for e in g:
            # for p1 and p2 in e
            found = False
            for p in e[0]:
                i = 0
                kwds_len = len(kwds)
                kwds_par_len = len(kwds_partial)
                while not found and i < kwds_len:
                    if kwds[i] in p.split(' '):
                        found = True
                    i += 1
                i = 0
                while not found and i < kwds_par_len:
                    if kwds_partial[i] in p:
                        found = True
                    i += 1
            if not found:
                g_res.append(e)
        res.append(g_res)
    return res


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


def combine_presenters(lsts):
    return [_combine_presenters(l) for l in lsts]


def combine_presenter_sublists(lsts):
    return [_combine_presenter_sublists(l) for l in lsts]


def _combine_presenters(lst):
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


def _combine_presenter_sublists(lst):
    for i, l in enumerate(lst):
        for j, m in enumerate(lst):
            if j == i:
                continue
            l_match = True
            for ll in l[0]:
                m_match = False
                for mm in m[0]:
                    if ll in mm:
                        m_match = True
                if not m_match:
                    l_match = False
            if l_match and l[1] * 0.75 < m[1]:
                lst[j][1] /= 0.75
    # prefer pairs by summing counts for individuals in pairs
    return sorted(lst, key=lambda x:x[1], reverse=True)


@profile
def rerank_nom(data, lsts):
    # re-rank based on occurrence frequency
    new_lsts = deepcopy(lsts)
    for l in new_lsts:
        for e in l: e[1] = 0
    for tweet in data:
        sent = tweet['text']
        for i, g in enumerate(new_lsts):
            for x in g:
                if x[0] in sent:
                    x[1] += 1
    candidates = [sorted(new_lsts[i], key=lambda x: x[1], reverse=True) for i in range(len(new_lsts))]

    return candidates


@profile
def rerank_ts(data, lsts, ts):
    # zero counts
    new_lsts = deepcopy(lsts)
    names_lst = []
    for i, lst in enumerate(new_lsts):
        names, _ = zip(*lst)
        names_lst.append(names)
    for l in new_lsts:
        for e in l: e[1] = 0
    for tweet in data:
        if 'dress' not in tweet['text'] or 'present' in tweet['text']:
            t = int(tweet['timestamp_ms'])
            i = 0
            ts_len = len(ts)
            while i < ts_len and t > ts[i]:
                i += 1
            i -= 1
            if i > -1:
                text = tweet['text']
                for j, x in enumerate(names_lst[i]):
                    for k, y in enumerate(x):
                        if y in text:
                            new_lsts[i][j][1] += 1
    '''# average counts for pairs
    for i, lst in enumerate(new_lsts):
        for j, x in enumerate(lst):
            new_lsts[i][j][1] /= len(new_lsts[i][j][0])'''
    return [sorted(lst, key=lambda x:x[1], reverse=True) for lst in new_lsts]


@profile
def rerank_ts_nominees(lsts, ts, data):
    # zero counts
    new_lsts = deepcopy(lsts)
    names_lst = []
    for i, lst in enumerate(new_lsts):
        names, _ = zip(*lst)
        names_lst.append(names)
    for l in new_lsts:
        for e in l: e[1] = 0
    for tweet in data:
        if 'dress' not in tweet['text'] or 'nomin' in tweet['text']:
            t = int(tweet['timestamp_ms'])
            i = 0
            ts_len = len(ts)
            while i < ts_len and t > ts[i]:
                i += 1
            i -= 1
            if i > -1:
                text = tweet['text']
                for j, x in enumerate(names_lst[i]):
                    if x in text:
                        new_lsts[i][j][1] += 1
    '''# average counts for pairs
    for i, lst in enumerate(new_lsts):
        for j, x in enumerate(lst):
            new_lsts[i][j][1] /= len(new_lsts[i][j][0])'''
    return [sorted(lst, key=lambda x:x[1], reverse=True) for lst in new_lsts]
