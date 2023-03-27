from data import *
from utils import *
import numpy as np
import ray
import time
from line_profiler_pycharm import profile


@profile
def extract_hosts(data):
    ca_set = []
    for tweet in data:
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
        for tweet in data:
            sent = tweet['text'].split()
            if is_Sublist(sent, x):
                n += 1
        ca_freq.append([x, n])
    sorted_ca_freq = sorted(ca_freq, key=lambda x :x[1], reverse=True)

    found = False
    hosts = None
    while not found:
        if sorted_ca_freq[0][0] == ['the'] or sorted_ca_freq[0][0] == ['golden', 'globes'] or sorted_ca_freq[0][0] == ['to']:
            sorted_ca_freq.remove(sorted_ca_freq[0])
        else:
            hosts = sorted_ca_freq[0][0]
            # print('Host(s): ' + ' '.join(hosts))
            found = True
    return hosts


@profile
def extract_awards(data):
    ca_set_awards = []
    for tweet in data:
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
        for tweet in data:
            sent = tweet['text'].split()
            if is_Sublist(sent, x):
                n += 1
        ca_freq_awards.append([x, n])

    sorted_ca_freq_awards = sorted(ca_freq_awards, key=lambda x :x[1], reverse=True)
    return sorted_ca_freq_awards


@ray.remote
def extract_winners(data, indices, awards):
    n_awards = len(awards)
    data_len = len(data)
    award_winner = [[] for _ in range(n_awards)]
    for tweet in data[indices[0]: indices[1]]:
        text = tweet['text']
        sent = text.split()
        for i, award in enumerate(awards):
            # get award names
            award_str = ' '.join(award)
            if award_str in text:
                j = text.find(award_str)
                j = len(text[:j].split())
                cand_winner_text = None
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
                elif j > 1 and sent[j - 1] == 'for':
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
                if cand_winner_text is not None and len(cand_winner_text) > 0:
                    award_winner[i].append([cand_winner_text, int(tweet['timestamp_ms'])])
    return award_winner


@ray.remote
def extract_nominees(data, indices):
    nominees = []
    for tweet in data[indices[0]: indices[1]]:
        sent = tweet['text'].split()
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
            nominees.append([nominee_text, int(tweet['timestamp_ms'])])
    return nominees


@ray.remote
def extract_presenters(data, indices):
    presenters = []
    for tweet in data[indices[0]: indices[1]]:
        sent = tweet['text'].split()
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
                if i > 0 and sent[i - 1] == 'to':
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
            presenters.append([presenter_text, int(tweet['timestamp_ms'])])
    return presenters


@profile
def main():
    ans = {"hosts": ["amy poehler", "tina fey"], "award_data": {"best screenplay - motion picture": {"nominees": ["zero dark thirty", "lincoln", "silver linings playbook", "argo"], "presenters": ["robert pattinson", "amanda seyfried"], "winner": "django unchained"}, "best director - motion picture": {"nominees": ["kathryn bigelow", "ang lee", "steven spielberg", "quentin tarantino"], "presenters": ["halle berry"], "winner": "ben affleck"}, "best performance by an actress in a television series - comedy or musical": {"nominees": ["zooey deschanel", "tina fey", "julia louis-dreyfus", "amy poehler"], "presenters": ["aziz ansari", "jason bateman"], "winner": "lena dunham"}, "best foreign language film": {"nominees": ["the intouchables", "kon tiki", "a royal affair", "rust and bone"], "presenters": ["arnold schwarzenegger", "sylvester stallone"], "winner": "amour"}, "best performance by an actor in a supporting role in a motion picture": {"nominees": ["alan arkin", "leonardo dicaprio", "philip seymour hoffman", "tommy lee jones"], "presenters": ["bradley cooper", "kate hudson"], "winner": "christoph waltz"}, "best performance by an actress in a supporting role in a series, mini-series or motion picture made for television": {"nominees": ["hayden panettiere", "archie panjabi", "sarah paulson", "sofia vergara"], "presenters": ["dennis quaid", "kerry washington"], "winner": "maggie smith"}, "best motion picture - comedy or musical": {"nominees": ["the best exotic marigold hotel", "moonrise kingdom", "salmon fishing in the yemen", "silver linings playbook"], "presenters": ["dustin hoffman"], "winner": "les miserables"}, "best performance by an actress in a motion picture - comedy or musical": {"nominees": ["emily blunt", "judi dench", "maggie smith", "meryl streep"], "presenters": ["will ferrell", "kristen wiig"], "winner": "jennifer lawrence"}, "best mini-series or motion picture made for television": {"nominees": ["the girl", "hatfields & mccoys", "the hour", "political animals"], "presenters": ["don cheadle", "eva longoria"], "winner": "game change"}, "best original score - motion picture": {"nominees": ["argo", "anna karenina", "cloud atlas", "lincoln"], "presenters": ["jennifer lopez", "jason statham"], "winner": "life of pi"}, "best performance by an actress in a television series - drama": {"nominees": ["connie britton", "glenn close", "michelle dockery", "julianna margulies"], "presenters": ["nathan fillion", "lea michele"], "winner": "claire danes"}, "best performance by an actress in a motion picture - drama": {"nominees": ["marion cotillard", "sally field", "helen mirren", "naomi watts", "rachel weisz"], "presenters": ["george clooney"], "winner": "jessica chastain"}, "cecil b. demille award": {"nominees": [], "presenters": ["robert downey, jr."], "winner": "jodie foster"}, "best performance by an actor in a motion picture - comedy or musical": {"nominees": ["jack black", "bradley cooper", "ewan mcgregor", "bill murray"], "presenters": ["jennifer garner"], "winner": "hugh jackman"}, "best motion picture - drama": {"nominees": ["django unchained", "life of pi", "lincoln", "zero dark thirty"], "presenters": ["julia roberts"], "winner": "argo"}, "best performance by an actor in a supporting role in a series, mini-series or motion picture made for television": {"nominees": ["max greenfield", "danny huston", "mandy patinkin", "eric stonestreet"], "presenters": ["kristen bell", "john krasinski"], "winner": "ed harris"}, "best performance by an actress in a supporting role in a motion picture": {"nominees": ["amy adams", "sally field", "helen hunt", "nicole kidman"], "presenters": ["megan fox", "jonah hill"], "winner": "anne hathaway"}, "best television series - drama": {"nominees": ["boardwalk empire", "breaking bad", "downton abbey (masterpiece)", "the newsroom"], "presenters": ["salma hayek", "paul rudd"], "winner": "homeland"}, "best performance by an actor in a mini-series or motion picture made for television": {"nominees": ["benedict cumberbatch", "woody harrelson", "toby jones", "clive owen"], "presenters": ["jessica alba", "kiefer sutherland"], "winner": "kevin costner"}, "best performance by an actress in a mini-series or motion picture made for television": {"nominees": ["nicole kidman", "jessica lange", "sienna miller", "sigourney weaver"], "presenters": ["don cheadle", "eva longoria"], "winner": "julianne moore"}, "best animated feature film": {"nominees": ["frankenweenie", "hotel transylvania", "rise of the guardians", "wreck-it ralph"], "presenters": ["sacha baron cohen"], "winner": "brave"}, "best original song - motion picture": {"nominees": ["act of valor", "stand up guys", "the hunger games", "les miserables"], "presenters": ["jennifer lopez", "jason statham"], "winner": "skyfall"}, "best performance by an actor in a motion picture - drama": {"nominees": ["richard gere", "john hawkes", "joaquin phoenix", "denzel washington"], "presenters": ["george clooney"], "winner": "daniel day-lewis"}, "best television series - comedy or musical": {"nominees": ["the big bang theory", "episodes", "modern family", "smash"], "presenters": ["jimmy fallon", "jay leno"], "winner": "girls"}, "best performance by an actor in a television series - drama": {"nominees": ["steve buscemi", "bryan cranston", "jeff daniels", "jon hamm"], "presenters": ["salma hayek", "paul rudd"], "winner": "damian lewis"}, "best performance by an actor in a television series - comedy or musical": {"nominees": ["alec baldwin", "louis c.k.", "matt leblanc", "jim parsons"], "presenters": ["lucy liu", "debra messing"], "winner": "don cheadle"}}}
    ans15 = {"hosts": ["amy poehler", "tina fey"], "award_data": {"best screenplay - motion picture": {"nominees": ["the grand budapest hotel", "gone girl", "boyhood", "the imitation game"], "presenters": ["bill hader", "kristen wiig"], "winner": "birdman"}, "best director - motion picture": {"nominees": ["wes anderson", "ava duvernay", "david fincher", "alejandro inarritu gonzalez"], "presenters": ["harrison ford"], "winner": "richard linklater"}, "best performance by an actress in a television series - comedy or musical": {"nominees": ["lena dunham", "edie falco", "julia louis-dreyfus", "taylor schilling"], "presenters": ["bryan cranston", "kerry washington"], "winner": "gina rodriguez"}, "best foreign language film": {"nominees": ["force majeure", "gett: the trial of viviane amsalem", "ida", "tangerines"], "presenters": ["colin farrell", "lupita nyong'o"], "winner": "leviathan"}, "best performance by an actor in a supporting role in a motion picture": {"nominees": ["robert duvall", "edward norton", "mark ruffalo"], "presenters": ["jennifer aniston", "benedict cumberbatch"], "winner": "j.k. simmons"}, "best performance by an actress in a supporting role in a series, mini-series or motion picture made for television": {"nominees": ["uzo aduba", "kathy bates", "allison janney", "michelle monaghan"], "presenters": ["jamie dornan", "dakota johnson"], "winner": "joanne froggatt"}, "best motion picture - comedy or musical": {"nominees": ["birdman", "into the woods", "pride", "st. vincent"], "presenters": ["robert downey, jr."], "winner": "the grand budapest hotel"}, "best performance by an actress in a motion picture - comedy or musical": {"nominees": ["emily blunt", "helen mirren", "julianne moore", "quvenzhane wallis"], "presenters": ["ricky gervais"], "winner": "amy adams"}, "best mini-series or motion picture made for television": {"nominees": ["the missing", "the normal heart", "olive kitteridge", "true detective"], "presenters": ["jennifer lopez", "jeremy renner"], "winner": "fargo"}, "best original score - motion picture": {"nominees": ["the imitation game", "birdman", "gone girl", "interstellar"], "presenters": ["sienna miller", "vince vaughn"], "winner": "the theory of everything"}, "best performance by an actress in a television series - drama": {"nominees": ["claire danes", "viola davis", "julianna margulies", "robin wright"], "presenters": ["anna faris", "chris pratt"], "winner": "ruth wilson"}, "best performance by an actress in a motion picture - drama": {"nominees": ["jennifer aniston", "felicity jones", "rosamund pike", "reese witherspoon"], "presenters": ["matthew mcconaughey"], "winner": "julianne moore"}, "cecil b. demille award": {"nominees": [], "presenters": ["don cheadle", "julianna margulies"], "winner": "george clooney"}, "best performance by an actor in a motion picture - comedy or musical": {"nominees": ["ralph fiennes", "bill murray", "joaquin phoenix", "christoph waltz"], "presenters": ["amy adams"], "winner": "michael keaton"}, "best motion picture - drama": {"nominees": ["foxcatcher", "the imitation game", "selma", "the theory of everything"], "presenters": ["meryl streep"], "winner": "boyhood"}, "best performance by an actor in a supporting role in a series, mini-series or motion picture made for television": {"nominees": ["alan cumming", "colin hanks", "bill murray", "jon voight"], "presenters": ["katie holmes", "seth meyers"], "winner": "matt bomer"}, "best performance by an actress in a supporting role in a motion picture": {"nominees": ["jessica chastain", "keira knightley", "emma stone", "meryl streep"], "presenters": ["jared leto"], "winner": "patricia arquette"}, "best television series - drama": {"nominees": ["downton abbey (masterpiece)", "game of thrones", "the good wife", "house of cards"], "presenters": ["adam levine", "paul rudd"], "winner": "the affair"}, "best performance by an actor in a mini-series or motion picture made for television": {"nominees": ["martin freeman", "woody harrelson", "matthew mcconaughey", "mark ruffalo"], "presenters": ["jennifer lopez", "jeremy renner"], "winner": "billy bob thornton"}, "best performance by an actress in a mini-series or motion picture made for television": {"nominees": ["jessica lange", "frances mcdormand", "frances o'connor", "allison tolman"], "presenters": ["kate beckinsale", "adrien brody"], "winner": "maggie gyllenhaal"}, "best animated feature film": {"nominees": ["big hero 6", "the book of life", "the boxtrolls", "the lego movie"], "presenters": ["kevin hart", "salma hayek"], "winner": "how to train your dragon 2"}, "best original song - motion picture": {"nominees": ["big eyes", "noah", "annie", "the hunger games: mockingjay - part 1"], "presenters": ["prince"], "winner": "selma"}, "best performance by an actor in a motion picture - drama": {"nominees": ["steve carell", "benedict cumberbatch", "jake gyllenhaal", "david oyelowo"], "presenters": ["gwyneth paltrow"], "winner": "eddie redmayne"}, "best television series - comedy or musical": {"nominees": ["girls", "jane the virgin", "orange is the new black", "silicon valley"], "presenters": ["bryan cranston", "kerry washington"], "winner": "transparent"}, "best performance by an actor in a television series - drama": {"nominees": ["clive owen", "liev schreiber", "james spader", "dominic west"], "presenters": ["david duchovny", "katherine heigl"], "winner": "kevin spacey"}, "best performance by an actor in a television series - comedy or musical": {"nominees": ["louis c.k.", "don cheadle", "ricky gervais", "william h. macy"], "presenters": ["jane fonda", "lily tomlin"], "winner": "jeffrey tambor"}}}

    global n_CPU
    data = load('./gg2013.json')
    global data_len
    data_len = len(data)
    global data_ref
    data_ref = ray.put(data)

    print(data_len)
    winners_raw = ray_data_workers(extract_winners, collect_combine_n, n_CPU, data_len, data_ref, awards)
    winner_grouped = ray_res_workers(untie_raw_winners, collect, winners_raw)
    print(winner_grouped)
    order = np.argsort([x[2] for x in winner_grouped])
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
    for i, r in enumerate(nominee_grouped):
        winner = ' '.join(winner_grouped[i][0])
        for x in r:
            if x[0] == winner:
                r.remove(x)

    # order by award list
    nom_target = [ans['award_data'][award_map_inv[' '.join(a)]]['nominees'] for a in awards]
    nom_res = [[] for _ in range(len(nom_target))]
    true = 0
    tot = 0
    final_nominees = []

    for i, award in enumerate(nom_target):
        for n in award:
            cand_list, _ = zip(*nominee_grouped[i])
            final_nominees.append(cand_list[:4])
            if n in cand_list[:5]:
                nom_res[i].append([n, True])
                true+=1
                tot+=1
            else:
                nom_res[i].append([n, False])
                tot+=1

    print(true/tot)
    for r in nom_res:
        print(r)

    for r in nominee_grouped:
        print(r)

    presenters_raw = ray_data_workers(extract_presenters, collect_combine, n_CPU, data_len, data_ref)
    ts_diff = np.diff(timestamps)
    timestamps_mid = [int(timestamps[0] - ts_diff[0]/2)]
    for i, dt in enumerate(ts_diff):
        if i == 0:
            timestamps_mid.append(timestamps_mid[i] + ts_diff[i])
        else:
            timestamps_mid.append(timestamps_mid[i] + ts_diff[i] / 2 + ts_diff[i - 1] / 2)

    # combine identical strings
    print("--- Begin unique_strs_ts() --- ")
    t = time.time()
    presenters = unique_strs_ts(presenters_raw, start=timestamps_mid[0])
    dt = time.time() - t
    print(f"--- End unique_strs_ts(), used {dt}ms --- ")
    # filter by award announcement time intervals
    presenters = ray_data_workers(filter_by_timestamp, collect_combine_n, n_CPU, data_len, presenters, timestamps_mid, True)
    # remove clearly irrelevant terms
    presenters = ray_res_workers(disqualify_kwd_str, collect, presenters)
    # combine identical items and merge sub-lists into super-lists
    presenters = ray_res_workers(combine_presenters, collect, presenters)
    # rerank by occurrence frequency within award timeslot
    presenters = ray_data_workers(rerank_ts, rerank_combine, n_CPU, data_len, data_ref, presenters, timestamps_mid)
    # soft combine by increasing the weight of super-strings when freq(sub-string)*0.75<freq(super-string)
    presenters = ray_res_workers(combine_presenter_sublists, collect, presenters)

    # order by award list
    presenter_grouped = [presenters[list(order).index(i)] for i in range(len(awards))]

    # order by award list
    p = [ans['award_data'][award_map_inv[' '.join(a)]]['presenters'] for a in awards]
    p_res = [[] for _ in range(len(p))]
    true = 0
    tot = 0
    for i, award in enumerate(p):
        for n in award:
            if len(presenter_grouped[i]) > 0:
                cand_list, _ = zip(*presenter_grouped[i])
                if n in np.concatenate(cand_list[:1]):
                    p_res[i].append([n, True])
                    true+=1
                    tot+=1
                else:
                    p_res[i].append([n, False])
                    tot+=1
            else:
                p_res[i].append([n, False])
                tot+= 1

    print(true/tot)

    for r in p_res:
        print(r)

    for r in presenter_grouped:
        print(r)


if __name__ == '__main__':
    main()
