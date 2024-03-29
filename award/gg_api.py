from main import *

'''Version 0.35'''

OFFICIAL_AWARDS_1315 = ['cecil b. demille award', 'best motion picture - drama', 'best performance by an actress in a motion picture - drama', 'best performance by an actor in a motion picture - drama', 'best motion picture - comedy or musical', 'best performance by an actress in a motion picture - comedy or musical', 'best performance by an actor in a motion picture - comedy or musical', 'best animated feature film', 'best foreign language film', 'best performance by an actress in a supporting role in a motion picture', 'best performance by an actor in a supporting role in a motion picture', 'best director - motion picture', 'best screenplay - motion picture', 'best original score - motion picture', 'best original song - motion picture', 'best television series - drama', 'best performance by an actress in a television series - drama', 'best performance by an actor in a television series - drama', 'best television series - comedy or musical', 'best performance by an actress in a television series - comedy or musical', 'best performance by an actor in a television series - comedy or musical', 'best mini-series or motion picture made for television', 'best performance by an actress in a mini-series or motion picture made for television', 'best performance by an actor in a mini-series or motion picture made for television', 'best performance by an actress in a supporting role in a series, mini-series or motion picture made for television', 'best performance by an actor in a supporting role in a series, mini-series or motion picture made for television']
OFFICIAL_AWARDS_1819 = ['best motion picture - drama', 'best motion picture - musical or comedy', 'best performance by an actress in a motion picture - drama', 'best performance by an actor in a motion picture - drama', 'best performance by an actress in a motion picture - musical or comedy', 'best performance by an actor in a motion picture - musical or comedy', 'best performance by an actress in a supporting role in any motion picture', 'best performance by an actor in a supporting role in any motion picture', 'best director - motion picture', 'best screenplay - motion picture', 'best motion picture - animated', 'best motion picture - foreign language', 'best original score - motion picture', 'best original song - motion picture', 'best television series - drama', 'best television series - musical or comedy', 'best television limited series or motion picture made for television', 'best performance by an actress in a limited series or a motion picture made for television', 'best performance by an actor in a limited series or a motion picture made for television', 'best performance by an actress in a television series - drama', 'best performance by an actor in a television series - drama', 'best performance by an actress in a television series - musical or comedy', 'best performance by an actor in a television series - musical or comedy', 'best performance by an actress in a supporting role in a series, limited series or motion picture made for television', 'best performance by an actor in a supporting role in a series, limited series or motion picture made for television', 'cecil b. demille award']

def get_hosts(year):
    '''Hosts is a list of one or more strings. Do NOT change the name
    of this function or what it returns.'''
    # Your code here
    hosts = host[year]
    return hosts

def get_awards(year):
    '''Awards is a list of strings. Do NOT change the name
    of this function or what it returns.'''
    # Your code here
    awards = award[year]
    return awards

def get_nominees(year):
    '''Nominees is a dictionary with the hard coded award
    names as keys, and each entry a list of strings. Do NOT change
    the name of this function or what it returns.'''
    # Your code here
    nominees = {}
    for i, a in enumerate(award_lst):
        formal_name = award_map_inv[' '.join(a)]
        nominees[formal_name] = nominee[year][i]
    return nominees

def get_winner(year):
    '''Winners is a dictionary with the hard coded award
    names as keys, and each entry containing a single string.
    Do NOT change the name of this function or what it returns.'''
    # Your code here
    winners = {}
    for i, a in enumerate(award_lst):
        formal_name = award_map_inv[' '.join(a)]
        winners[formal_name] = winner[year][i]
    return winners

def get_presenters(year):
    '''Presenters is a dictionary with the hard coded award
    names as keys, and each entry a list of strings. Do NOT change the
    name of this function or what it returns.'''
    # Your code here
    presenters = {}
    for i, a in enumerate(award_lst):
        formal_name = award_map_inv[' '.join(a)]
        presenters[formal_name] = presenter[year][i]
    return presenters

def pre_ceremony():
    '''This function loads/fetches/processes any data your program
    will use, and stores that data in your DB or in a json, csv, or
    plain text file. It is the first thing the TA will run when grading.
    Do NOT change the name of this function or what it returns.'''
    # Your code here
    global n_CPU
    global data_len2013
    global data_ref2013
    global data_len2015
    global data_ref2015
    n_CPU = 4
    ray.init(num_cpus=n_CPU)
    data2013 = load('./gg2013.json', n_CPU)
    data2015 = load('./gg2015.json', n_CPU)
    data_len2013 = len(data2013)
    data_len2015 = len(data2015)
    data_ref2013 = ray.put(data2013)
    data_ref2015 = ray.put(data2015)

    print("Pre-ceremony processing complete.")
    return

def main():
    '''This function calls your program. Typing "python gg_api.py"
    will run this function. Or, in the interpreter, import gg_api
    and then run gg_api.main(). This is the second thing the TA will
    run when grading. Do NOT change the name of this function or
    what it returns.'''
    # Your code here
    global host
    global award
    global nominee
    global presenter
    global winner
    host = {}
    award = {}
    nominee = {}
    presenter = {}
    winner = {}
    host['2013'], award['2013'], winner['2013'], nominee['2013'], presenter['2013'] = process(data_len2013,
                                                                                              data_ref2013,
                                                                                              n_CPU)
    host['2015'], award['2015'], winner['2015'], nominee['2015'], presenter['2015'] = process(data_len2015,
                                                                                              data_ref2015,
                                                                                              n_CPU)
    award_lst.append(['cecil', 'b.', 'demille', 'award'])
    for y in ['2013', '2015']:
        print(f'\nFor the Golden Globes Award Ceremony in {y}:')
        json_output = {}
        json_output['hosts'] = host[y]
        json_output['award_data'] = {}
        print('Host(s): ' + ', '.join(capitalize(host[y])))
        print('Awards identified (top 25):\n\t' + '\n\t'.join(capitalize(award[y])))
        print('\nUsing hardcoded award names, the following award information was found:')
        for i, a in enumerate(award_lst):
            formal_name = award_map_inv[' '.join(a)]
            print('\nAward:\n\t' + capitalize([formal_name])[0])
            print('Presenter(s):\n\t' + ', '.join(capitalize(presenter[y][i])))
            print('Nominees:\n\t' + '\n\t'.join(capitalize(nominee[y][i])))
            print('Winner:\n\t' + capitalize([winner[y][i]])[0])
            json_output['award_data'][formal_name] = {}
            json_output['award_data'][formal_name]['presenters'] = presenter[y][i]
            json_output['award_data'][formal_name]['nominees'] = nominee[y][i]
            json_output['award_data'][formal_name]['winner'] = winner[y][i]
    return


if __name__ == '__main__':
    main()
