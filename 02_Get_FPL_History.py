#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Fetching FPL player data from the official EPL api

"""

__author__ = 'Micah Cearns'
__contact__ = 'micahcearns@gmail.com'
__date__ = 'August 2020'

import json
import os
import time
import warnings
import pandas as pd
import progressbar
from tqdm import tqdm
import requests


OUTPUT_PATH = '/Users/MicahJackson/Desktop/fpl-optimiser-master/Output'
DK_OUTPUT_PATH = ('/Users/MicahJackson/anaconda/Pycharm_WD/Draft_Kings_EPL_'
                  'Project/Output')

def fetch_player_history(player_id):
    """

    Fetch JSON of a single player's FPL history

    :param player_id: Individual Player ids as integers

    :return player_history: Dictionary of a players history within the EPL and
                            all subsequent official EPL fantasy metrics

    """
    url = ('https://fantasy.premierleague.com/api/element-summary/{}/'
           .format(player_id))
    r = requests.get(url)
    return r.json()['history_past']


def fetch_all_player_histories(max_id=1000):
    """

    Fetch the histories of all EPL players

    :param max_id: Integer ids to iterate through for EPL players

    :return histories: Dictionaries of individual EPL player data for all
                       their seasons in the EPL
    """
    histories = []
    bar = progressbar.ProgressBar()
    for player_id in tqdm(range(1, max_id+1), desc='Get player histories'):
        try:
            history = fetch_player_history(player_id)
            histories += history
        except json.decoder.JSONDecodeError:
            print('\nLast player found at id = {0}'.format(player_id - 1))
            return histories
        except KeyError:  # Catching key errors for players missing data
            pass
        time.sleep(0.5)  # To avoid overloading their servers
    else:
        warnings.warn('Last player_id not reached. You ought to try again '
                      'with a higher max_id')
        return histories


def fetch_positions():
    """ Fetch table mapping position_ids to position names. """
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url)
    positions = r.json()['element_types']
    return positions


def fetch_player_info():
    """ Fetch player info for the most recent season. """
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url)

    positions = []
    for player in r.json()['elements']:
        positions.append({
            'position_id': player['element_type'],
            'player_id': player['code'],
            'team_id': player['team_code'],
            'full_name': player['first_name'] + ' ' + player['second_name'],
            'now_cost': player['now_cost'],
            'selected_by': player['selected_by_percent'],
            'player_form' :player['form'],  # ADDED THESE 4 ELEMENTS IN
            'value_to_form_ratio': player['value_form'],
            'player_news': player['news'],
            'chance_of_playing_next_round': player['chance_of_playing'
                                                   '_next_round']
        })
    return positions


def fetch_and_save_history(max_id=1000):
    """ Fetch and save all historical seasons """
    scores = pd.DataFrame(fetch_all_player_histories(max_id))
    players = pd.DataFrame(fetch_player_info())
    positions = pd.DataFrame(fetch_positions())

    print(players)

    # Add position info and clean up columns
    history = scores.merge(players,
                           how='outer',
                           left_on='element_code',
                           right_on='player_id')

    history = history.merge(positions,
                            how='outer',
                            left_on='position_id',
                            right_on='id')

    columns = ['player_id',
               'full_name',
               'team_id',
               'singular_name',
               'start_cost',
               'end_cost',
               'now_cost',
               'total_points',
               'season_name',
               'minutes',
               'bonus',
               'bps',
               'goals_scored',
               'assists',
               'selected_by',
               'goals_conceded',
               'clean_sheets',
               'yellow_cards',
               'red_cards',
               'penalties_missed',
               'saves',
               'penalties_saved',
               'player_form',
               'value_to_form_ratio',
               'player_news',
               'chance_of_playing_next_round']

    history = history[columns]
    history = history.rename(columns={'singular_name': 'position',
                                      'bps': 'bonus_points'})

    positions.to_csv(os.path.join(OUTPUT_PATH,'Positions.csv'),
                     index=False,
                     encoding='utf-8')

    history.to_csv(os.path.join(OUTPUT_PATH,'FPL_history.csv'),
                   index=False,
                   encoding='utf-8')

    history.to_csv(os.path.join(DK_OUTPUT_PATH,'FPL_history.csv'),
                   index=False,
                   encoding='utf-8')


if __name__ == '__main__':
    print('Running code')
    fetch_and_save_history()
