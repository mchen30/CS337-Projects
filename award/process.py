from data import *
from utils import *
import numpy as np
import ray
import sys


@ray.remote
def extract_hosts(data, indices):
    ca_set = []
    sents = [x.split() for x in data.iloc[range(*indices)]['text'].tolist()]
    for sent in sents:
        for i, word in enumerate(sent):
            # hosted by xxxxxxxx
            if word == 'hosted' and i+1 < len(sent) and sent[i+1] == 'by':
                ca_set += look_forward(sent, i+1, max_len=5)
            # xxxxxxxxx host(s)
            elif word == 'host' or word == 'hosts':
                ca_set += look_backward(sent, i, max_len=5)
            elif word == 'hosts' and i > 1 and sent[i-1] == 'the' and sent[i-2] == 'are':
                ca_set += look_backward(sent, i-2, max_len=5)
            elif  word == 'hosts' and i > 1 and sent[i-2] == 'are':
                ca_set += look_backward(sent, i-1, max_len=5)
            elif word == 'host' and i > 1 and sent[i-1] == 'the' and sent[i-2] == 'is':
                ca_set += look_backward(sent, i-2, max_len=5)
            # xxxxxxxxx is/are hosting
            elif word == 'hosting' and i > 0 and (sent[i-1] == 'is' or sent[i-1] == 'are'):
                ca_set += look_backward(sent, i-1, max_len=5)
            elif word == 'hosting' and i > 0 and sent[i-1] != 'is' and sent[i-1] != 'are':
                ca_set += look_backward(sent, i, max_len=5)
    return ca_set


@ray.remote
def extract_awards(data, indices):
    ca_set_awards = []
    sents = [x.split() for x in data.iloc[range(*indices)]['text'].tolist()]
    for sent in sents:
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
    return ca_set_awards


@ray.remote
def extract_winners(data, indices, awards):
    n_awards = len(awards)
    data_len = len(data)
    award_winner = [[] for _ in range(n_awards)]
    demille_winner = []
    texts = data.iloc[range(*indices)]['text'].tolist()
    tss = data.iloc[range(*indices)]['timestamp_ms'].tolist()
    sents = [x.split() for x in texts]
    for k, sent in enumerate(sents):
        text = texts[k]
        ts = tss[k]
        for i, award in enumerate(awards):
            j = text.find('cecil b demille award')
            if j != -1:
                demille_winner_text = []
                j = len(text[:j].split())
                # extract demille winner
                demille_winner_text += look_backward(sent, j, end=['wins', 'the'], include=False, max_len=3)
                demille_winner_text += look_backward(sent, j, end=['wins'], include=False, max_len=3)
                demille_winner_text += look_backward(sent, j, end=['will', 'receive', 'the'], include=False, max_len=3)
                demille_winner_text += look_backward(sent, j, end=['will', 'receive'], include=False, max_len=3)
                demille_winner_text += look_backward(sent, j, end=['for', 'winning', 'the'], include=False, max_len=3)
                demille_winner_text += look_backward(sent, j, end=['for', 'winning'], include=False, max_len=3)
                demille_winner_text += look_forward(sent, j + 4, start=['goes', 'to'], include=False, max_len=3)
                demille_winner_text += look_forward(sent, j + 4, start=['to'], include=False, max_len=3)
                if len(demille_winner_text) > 0:
                    demille_winner.append([demille_winner_text, ts])
            # get award names
            award_str = ' '.join(award)
            j = text.find(award_str)
            if j != -1:
                j = len(text[:j].split())
                cand_winner_text = []
                # AWARD goes to xxx
                if j + len(award) + 2 < len(sent) and sent[j + len(award)] == 'goes' and sent[j + len(award) + 1] == 'to':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['goes', 'to'], include=False)
                # AWARD (#goldenglobe) awarded to xxx
                elif j + len(award) + 3 < len(sent) and sent[j + len(award)] == 'awarded' and sent[j + len(award) + 1] == 'to':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['awarded', 'to'], include=False)
                # AWARD is awarded to xxx
                elif j + len(award) + 3 < len(sent) and sent[j + len(award)] == 'is' and sent[j + len(award) + 1] == 'awarded' and sent[j + len(award) + 2] == 'to':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['is', 'awarded', 'to'], include=False)
                # winner for AWARD is xxx
                elif j > 2 and sent[j - 1] == 'for' and sent[j - 2] == 'winner' and len(sent) > j + len(award) + 1 and sent[j + len(award)] == 'is':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['is'], include=False)
                # AWARD is xxx
                elif len(sent) > j + len(award) and sent[j + len(award)] == 'is':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['is'], include=False)
                # someone presents AWARD to xxx
                elif j > 2 and len(sent) > j + len(award) and sent[j + len(award)] == 'to' and sent[j - 1] == 'presents':
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start=['to'], include=False)
                # xxx wins/won/has won the #goldenglobe for AWARD
                elif j > 4 and sent[j - 1] == 'for' and sent[j - 2] == 'the' and sent[j - 3] == 'wins':
                    cand_winner_text = look_backward(sent, j, end=['wins', 'the', 'for'], include=False)
                elif j > 4 and sent[j - 1] == 'for' and sent[j - 2] == 'the' and sent[j - 3] == 'won':
                    cand_winner_text = look_backward(sent, j, end=['won', 'the', 'for'], include=False)
                elif j > 5 and sent[j - 1] == 'for' and sent[j - 2] == 'the' and sent[j - 3] == 'won' and sent[j - 4] == 'has':
                    cand_winner_text = look_backward(sent, j, end=['has', 'won', 'the', 'for'], include=False)
                # xxx wins/won/has won the golden globe for AWARD
                elif j > 4 and sent[j - 1] == 'for' and sent[j - 4] == 'the' and sent[j - 5] == 'wins':
                    cand_winner_text = look_backward(sent, j, end=['wins', 'the', 'golden', 'globe', 'for'], include=False)
                elif j > 4 and sent[j - 1] == 'for' and sent[j - 4] == 'the' and sent[j - 5] == 'won':
                    cand_winner_text = look_backward(sent, j, end=['won', 'the', 'golden', 'globe', 'for'], include=False)
                elif j > 5 and sent[j - 1] == 'for' and sent[j - 4] == 'the' and sent[j - 5] == 'won' and sent[j - 6] == 'has':
                    cand_winner_text = look_backward(sent, j, end=['has', 'won', 'the', 'golden', 'globe', 'for'], include=False)
                # xxx - #goldenglobe winner for AWARD
                elif j > 3 and sent[j - 1] == 'for' and sent[j - 2] == 'winner':
                    cand_winner_text = look_backward(sent, j, end=['winner', 'for'], include=False)
                # congrat/congrats to xxx for her/his golden globe win as AWARD
                elif j > 6 and sent[j - 1] == 'as' and sent[j - 2] == 'win' and sent[j - 3] == 'globe' and sent[j - 4] == 'golden' and sent[j - 5] == 'his' and sent[j - 6] == 'for':
                    cand_winner_text = look_backward(sent, j, end=['for', 'his', 'golden', 'globe', 'win', 'as'], include=False)
                elif j > 6 and sent[j - 1] == 'as' and sent[j - 2] == 'win' and sent[j - 3] == 'globe' and sent[j - 4] == 'golden' and sent[j - 5] == 'her' and sent[j - 6] == 'for':
                    cand_winner_text = look_backward(sent, j, end=['for', 'her', 'golden', 'globe', 'win', 'as'], include=False)
                # xxx for AWARD
                elif j > 1 and sent[j - 1] == 'for' and sent[j - 2] != 'wins':
                    cand_winner_text = look_backward(sent, j, end=['for'], include=False)
                # xxx wins/won/has won AWARD
                elif j > 1 and sent[j - 1] == 'wins':
                    cand_winner_text = look_backward(sent, j, end=['wins'], include=False)
                elif j > 1 and sent[j - 1] == 'won':
                    cand_winner_text = look_backward(sent, j, end=['won'], include=False)
                elif j > 2 and sent[j - 1] == 'won' and sent[j - 2] == 'has':
                    cand_winner_text = look_backward(sent, j, end=['has', 'won'], include=False)
                # AWARD: xxx
                elif data_len < 500000 and j + len(award) < len(sent):
                    cand_winner_text = look_forward(sent, j + len(award) - 1, start_exclude=['at'])
                if len(cand_winner_text) > 0:
                    award_winner[i].append([cand_winner_text, ts])
    award_winner.append(demille_winner)
    return award_winner


@ray.remote
def extract_nominees(data, indices):
    nominees = []
    tss = data.iloc[range(*indices)]['timestamp_ms'].tolist()
    sents = [x.split() for x in data.iloc[range(*indices)]['text'].tolist()]
    for k, sent in enumerate(sents):
        ts = tss[k]
        nominee_text = []
        for j, word in enumerate(sent):
            if word == 'won' and j > 0:
                if sent[j-1] == 'have':
                    if j>2 and (sent[j-3] == 'seriously' or sent[j-3] == 'completely' or sent[j-3] == 'definitely' or sent[j-3] == 'entirely' or sent[j-3] == 'totally' or sent[j-3] == 'totes'):
                        nominee_text += look_backward(sent, j, end=['seriously', 'should', 'have'], include=False)
                        nominee_text += look_backward(sent, j, end=['definitely', 'should', 'have'], include=False)
                        nominee_text += look_backward(sent, j, end=['entirely', 'should', 'have'], include=False)
                        nominee_text += look_backward(sent, j, end=['completely', 'should', 'have'], include=False)
                        nominee_text += look_backward(sent, j, end=['totally', 'should', 'have'], include=False)
                        nominee_text += look_backward(sent, j, end=['totes', 'should', 'have'], include=False)
                    else:
                        nominee_text += look_backward(sent, j, end=['should', 'have'], include=False)
                else:
                    if j>1 and (sent[j - 2] == 'seriously' or sent[j - 2] == 'completely' or sent[j - 2] == 'definitely' or sent[j - 2] == 'entirely' or sent[j - 2] == 'totally' or sent[j - 2] == 'totes'):
                        nominee_text += look_backward(sent, j, end=['seriously', 'shouldve'], include=False)
                        nominee_text += look_backward(sent, j, end=['definitely', 'shouldve'], include=False)
                        nominee_text += look_backward(sent, j, end=['entirely', 'shouldve'], include=False)
                        nominee_text += look_backward(sent, j, end=['completely', 'shouldve'], include=False)
                        nominee_text += look_backward(sent, j, end=['totally', 'shouldve'], include=False)
                        nominee_text += look_backward(sent, j, end=['totes', 'shouldve'], include=False)
                    else:
                        nominee_text += look_backward(sent, j, end=['shouldve'], include=False)
            elif word == 'deserved' and j > 0 and (sent[j-1] == 'seriously' or sent[j-1] == 'completely' or sent[j-1] == 'definitely' or sent[j-1] == 'entirely' or sent[j-1] == 'totally' or sent[j-1] == 'totes'):
                if j < len(sent) - 1:
                    nominee_text += look_backward(sent, j + 2, end=['seriously', 'deserved', 'that'], include=False)
                    nominee_text += look_backward(sent, j + 2, end=['completely', 'deserved', 'that'], include=False)
                    nominee_text += look_backward(sent, j + 2, end=['definitely', 'deserved', 'that'], include=False)
                    nominee_text += look_backward(sent, j + 2, end=['entirely', 'deserved', 'that'], include=False)
                    nominee_text += look_backward(sent, j + 2, end=['totally', 'deserved', 'that'], include=False)
                    nominee_text += look_backward(sent, j + 2, end=['totes', 'deserved', 'that'], include=False)
                if j < len(sent) - 2:
                    nominee_text += look_backward(sent, j + 3, end=['seriously', 'deserved', 'to', 'win'], include=False)
                    nominee_text += look_backward(sent, j + 3, end=['completely', 'deserved', 'to', 'win'], include=False)
                    nominee_text += look_backward(sent, j + 3, end=['definitely', 'deserved', 'to', 'win'], include=False)
                    nominee_text += look_backward(sent, j + 3, end=['entirely', 'deserved', 'to', 'win'], include=False)
                    nominee_text += look_backward(sent, j + 3, end=['totally', 'deserved', 'to', 'win'], include=False)
                    nominee_text += look_backward(sent, j + 3, end=['totes', 'deserved', 'to', 'win'], include=False)
            elif word == 'deserved':
                if j < len(sent) - 1:
                    nominee_text += look_backward(sent, j + 2, end=['deserved', 'that'], include=False)
                if j < len(sent) - 2:
                    nominee_text += look_backward(sent, j + 3, end=['deserved', 'to', 'win'], include=False)
            elif word == 'wanted':
                nominee_text += look_forward(sent, j, end=['to', 'win'], include=False)
            elif word == 'win':
                nominee_text += look_backward(sent, j, end=["didnt"], include=False)
            elif word == 'robbed':
                nominee_text += look_backward(sent, j, end=["was"], include=False)
                nominee_text += look_backward(sent, j, end=["got"], include=False)
            elif word == 'beat':
                nominee_text += look_forward(sent, j, start=['out'], include=False)
            elif word == 'beats':
                nominee_text += look_forward(sent, j, start=['out'], include=False)
            elif word == 'no':
                nominee_text += look_forward(sent, j, start=['win', 'for'], include=False)
            elif word == 'up' and j < len(sent) - 1 and sent[j + 1] == 'for':
                nominee_text += look_backward(sent, j, end=['is'], include=False)
            elif word == 'why' and j < len(sent) - 2 and sent[j + 1] == 'not':
                nominee_text += look_forward(sent, j+1)
        if len(nominee_text) > 0:
            nominees.append([nominee_text, ts])
    return nominees


@ray.remote
def extract_presenters_demille(data, indices):
    presenters = []
    texts = data.iloc[range(*indices)]['text'].tolist()
    tss = data.iloc[range(*indices)]['timestamp_ms'].tolist()
    sents = [x.split() for x in texts]
    for k, sent in enumerate(sents):
        text = texts[k]
        ts = tss[k]
        j = text.find('cecil b demille award')
        if j != -1:
            j = len(text[:j].split())
            if j > 2 and sent[j - 1] == 'the' and (sent[j - 2] == 'presents' or sent[j - 2] == 'present'):
                p = find_presenters(sent[:j - 2])
                if len(p) > 0: presenters.append([p, ts])
            elif j > 1 and (sent[j - 1] == 'presents' or sent[j - 1] == 'present'):
                p = find_presenters(sent[:j - 1])
                if len(p) > 0: presenters.append([p, ts])
            elif j > 2 and sent[j - 1] == 'the' and sent[j - 2] == 'presenting':
                p = find_presenters(sent[:j - 2])
                if len(p) > 0: presenters.append([p, ts])
            elif j > 1 and sent[j - 1] == 'presenting':
                p = find_presenters(sent[:j - 1])
                if len(p) > 0: presenters.append([p, ts])
    return presenters


@ray.remote
def extract_presenters(data, indices):
    presenters = []
    tss = data.iloc[range(*indices)]['timestamp_ms'].tolist()
    sents = [x.split() for x in data.iloc[range(*indices)]['text'].tolist()]
    for k, sent in enumerate(sents):
        ts = tss[k]
        presenter_text = []
        for i, word in enumerate(sent):
            # x is/are presenting
            if word == 'presenting' and i > 0:
                if sent[i - 1] == 'are':
                    p = find_presenters(sent[:i - 1])
                    if len(p) > 0: presenter_text.extend(p)
                elif sent[i - 1] == 'is':
                    p = find_presenters(sent[:i - 1])
                    if len(p) > 0: presenter_text.extend(p)
                else:
                    p = find_presenters(sent[:i])
                    if len(p) > 0: presenter_text.extend(p)
            # AWARD (is being) presented by
            elif word == 'presented' and i < len(sent) - 1 and sent[i + 1] == 'by':
                p = find_presenters(sent[i + 2:], backward=False)
                if len(p) > 0: presenter_text.extend(p)
            elif word == 'presentan' or word == 'presenta':
                p = find_presenters(sent[:i], eng=False)
                if len(p) > 0: presenter_text.extend(p)
            # x presents
            # x and x present
            # x and x to present
            elif word == 'present' or word == 'presents':
                if i > 0 and (sent[i - 1] == 'to' or sent[i - 1] == 'will'):
                    continue
                else:
                    p = find_presenters(sent[:i])
                    if len(p) > 0: presenter_text.extend(p)
            # presenters x and x
            # presenter x
            elif word == 'presenters' or word == 'presenter':
                p = find_presenters(sent[i + 1:], backward=False)
                if len(p) > 0: presenter_text.extend(p)

        if len(presenter_text) > 0:
            presenters.append([presenter_text, ts])
    return presenters


def process(data_len, data_ref, n_CPU=4):
    ray.init(num_cpus=n_CPU, ignore_reinit_error=True)
    hosts_raw = ray_data_workers(extract_hosts, collect_combine, n_CPU, data_len, data_ref)
    sorted_ca = unique_ngrams(hosts_raw)[:50]  # select only top fifty
    sorted_ca = remove_duplicate_sublist(sorted_ca)
    # remove all sub-lists, re-rank based on occurrence frequency in the full dataset
    ca = remove_all_sublists(sorted_ca)
    if data_len < 500000:
        host_cand = ray_data_workers(rerank, combine_sort, n_CPU, data_len, data_ref, ca)
        hosts = filter_host_kwd(host_cand)
    else:
        hosts = ' '.join(ca[0]).split(' and ')

    awards_raw = ray_data_workers(extract_awards, collect_combine, n_CPU, data_len, data_ref)
    sorted_ca_awards = unique_ngrams(awards_raw)[:100]
    sorted_ca_awards = remove_duplicate_sublist(sorted_ca_awards)

    # remove all sub-lists, re-rank based on occurrence frequency in the full dataset
    award_cand = remove_all_sublists(sorted_ca_awards)
    award_cand = filter_award_kwd(award_cand)
    award_cand = ray_data_workers(rerank, combine_sort, n_CPU, data_len, data_ref, award_cand)[:25]
    award_cand = [' '.join(cand[0]) for cand in award_cand]

    winners_raw = ray_data_workers(extract_winners, collect_combine_n, n_CPU, data_len, data_ref, award_lst)
    winner_grouped = ray_res_workers(untie_raw_winners, collect, winners_raw)
    winner_lsts, _, _ = zip(*winner_grouped)
    # cecil demille award is at the end of list
    winners = [' '.join(x) for x in winner_lsts]

    order = np.argsort([x[2] for x in winner_grouped[:-1]])
    timestamps = [winner_grouped[i][2] for i in order]

    nominees_raw = ray_data_workers(extract_nominees, collect_combine, n_CPU, data_len, data_ref)
    nominees_unique = unique_ngrams_ts(nominees_raw, start=timestamps[0])
    nominees_ref = ray.put(nominees_unique)
    nominees = ray_data_workers(filter_by_timestamp, collect_combine_n, n_CPU, data_len, nominees_ref, timestamps, False)
    # order by award list
    nominees_ordered = [nominees[list(order).index(i)] for i in range(len(nominees))]
    nominees_filtered = ray_res_workers(disqualify_kwd, collect, nominees_ordered)
    nominees_dedup = ray_res_workers(remove_duplicate_sublist_str, collect, nominees_filtered)
    nominee_grouped = ray_res_workers(remove_dup_single, collect, nominees_dedup)
    nominee_grouped = ray_data_workers(rerank_nominees, rerank_combine, n_CPU, data_len, data_ref, nominee_grouped)

    # remove winners from nominees
    for i, noms in enumerate(nominee_grouped):
        remove = []
        for j, nom in enumerate(noms):
            if nom[0] == winners[i]:
                remove.append(j)
        remove = sorted(remove, reverse=True)
        for idx in remove:
            noms.remove(noms[idx])

    nominees = [[nom[0] for nom in noms[:4]] for noms in nominee_grouped]

    presenters_raw = ray_data_workers(extract_presenters, collect_combine, n_CPU, data_len, data_ref)
    demille_presenters_raw = ray_data_workers(extract_presenters_demille, collect_combine, n_CPU, data_len, data_ref)

    ts_diff = np.diff(timestamps)
    timestamps_mid = [int(timestamps[0] - ts_diff[0]/2)]
    for i, dt in enumerate(ts_diff):
        if i == 0:
            timestamps_mid.append(timestamps_mid[i] + ts_diff[i])
        else:
            timestamps_mid.append(timestamps_mid[i] + ts_diff[i] / 2 + ts_diff[i - 1] / 2)

    demille_presenters = unique_strs_ts(demille_presenters_raw)
    demille_presenters = ray_res_workers(combine_presenters, collect, [demille_presenters])[0]
    demille_presenters = ray_res_workers(combine_presenter_sublists, collect, [demille_presenters])[0][0][0]

    presenters = unique_strs_ts(presenters_raw, start=timestamps_mid[0])
    # filter by award announcement time intervals
    presenters = ray_data_workers(filter_by_timestamp, collect_combine_n, n_CPU, data_len, presenters, timestamps_mid, data_len<500000)
    # remove clearly irrelevant terms
    presenters = ray_res_workers(disqualify_kwd_str, collect, presenters, hosts)
    # combine identical items and merge sub-lists into super-lists
    presenters = ray_res_workers(combine_presenters, collect, presenters)
    # rerank by occurrence frequency within award timeslot
    presenters = ray_data_workers(rerank_ts, rerank_combine, n_CPU, data_len, data_ref, presenters, timestamps_mid)
    # soft combine by increasing the weight of super-strings when freq(sub-string)*0.75<freq(super-string)
    presenters = ray_res_workers(combine_presenter_sublists, collect, presenters)

    # order by award list
    presenter_grouped = [presenters[list(order).index(i)] for i in range(len(award_lst))]
    # order by award list
    presenters = [cands[0][0] for cands in presenter_grouped]

    award_lst.append(['cecil', 'b.', 'demille', 'award'])
    nominees.append([])
    presenters.append(demille_presenters)

    eval_nominees(nominees, 2013, award_map_inv, award_lst)
    eval_presenters(presenters, 2013, award_map_inv, award_lst)

    return hosts, award_cand, winners, nominees, presenters


if __name__ == '__main__':
    n_CPU = 4
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = '2015'
    data = load(f'./gg{year}.json', n_CPU)
    data_len = len(data)
    data_ref = ray.put(data)
    hosts, award_cand, winners, nominees, presenters = process(data_len, data_ref, n_CPU)
    json_output = {}
    json_output['hosts'] = hosts
    json_output['award_data'] = {}
    print('Host(s): ' + ', '.join(capitalize(hosts)))
    print('Awards identified (top 25):\n\t' + '\n\t'.join(capitalize(award_cand)))
    print('\nUsing hardcoded award names, the following award information was found:')
    for i, award in enumerate(award_lst):
        formal_name = award_map_inv[' '.join(award)]
        print('\nAward:\n\t' + capitalize([formal_name])[0])
        print('Presenter(s):\n\t' + ', '.join(capitalize(presenters[i])))
        print('Nominees:\n\t' + '\n\t'.join(capitalize(nominees[i])))
        print('Winner:\n\t' + capitalize([winners[i]])[0])
        json_output['award_data'][formal_name] = {}
        json_output['award_data'][formal_name]['presenters'] = presenters[i]
        json_output['award_data'][formal_name]['nominees'] = nominees[i]
        json_output['award_data'][formal_name]['winner'] = winners[i]
