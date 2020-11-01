#!/usr/bin/env python
# _*_ coding: utf-8 _*_

"""

Fetching player and fixture data from the football api

"""

__author__ = 'Micah Cearns'
__contact__ = 'micahcearns@gmail.com'
__date__ = 'August 2020'

import requests
import pandas as pd
import os
from tqdm import tqdm

OUTPUT_PATH = ('/Users/MicahJackson/anaconda/Pycharm_WD/Draft_Kings_EPL_Project'
               '/Output')


def connect_to_api():
    """ Connect to the rapid football api """
    headers = {
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
        'x-rapidapi-key': ""
    }
    return headers


def get_league_id(headers,
                  league='Premier League',
                  country='England',
                  year=2020):
    """

    Get league id to identify fixtures and players

    :param headers: API headers
    :param league: Global football league
    :param country: Country of league
    :param year: Year of league season

    :return league_id: Integer value representing the league id
    """
    league_url = 'https://api-football-v1.p.rapidapi.com/v2/leagues'
    response_league = requests.request("GET", league_url, headers=headers)
    league_df = pd.json_normalize(response_league.json()['api']['leagues'])
    epl_df = (league_df
              .loc[(league_df['name'] == league)
                   & (league_df['country'] == country)]
              .sort_values(by='season'))
    epl_df.to_csv(os.path.join(OUTPUT_PATH, 'EPL_df.csv'))
    league_id = (epl_df.loc[epl_df['season'] == year]['league_id'].iloc[0])

    return league_id


def get_fixture_ids(league_id, headers):
    """

    Get fixture ids for fixtures within a league and season

    :param league_id: League id value as an integer
    :param headers: API headers

    :return fixture_ids: Integer ids for each fixture within season
    """
    epl_url = ('https://api-football-v1.p.rapidapi.com/v2/fixtures/league/'
               + league_id.astype(str))
    response_epl = requests.request('GET', epl_url, headers=headers)
    epl_fixture_id_df = pd.json_normalize(response_epl.json()['api']['fixtures'])
    fixture_ids = epl_fixture_id_df['fixture_id'].astype(str).tolist()

    return fixture_ids


def get_player_data(fixture_ids, headers):
    """

    Get individual player data by fixture within league season

    :param fixture_ids: Integer fixture ids value for fixtures in season
    :param headers: API headers

    :return player_fixture_df: Pandas dataframe of individual players and
                               their metrics per game within a league season
    """
    url = "https://api-football-v1.p.rapidapi.com/v2/players/fixture/"
    player_list = []
    for id in tqdm(fixture_ids, desc='Getting player data'):
        response = requests.request("GET", url + id, headers=headers)
        results_n = response.json()['api']['results']  # Getting n players
        results_n_range = range(0, results_n, 1)
        for n in results_n_range:  # Looping through all players in each fixture
            try:
                player = response.json()['api']['players'][n]
            except KeyError:
                pass
            player_list.append(player)

    player_fixture_df = pd.json_normalize(player_list)
    player_fixture_df.to_csv(os.path.join(OUTPUT_PATH,
                                          'Player_fixture_df_2020.csv'))

    return player_fixture_df


if __name__ == '__main__':
    headers = connect_to_api()  # EPL league id is 524
    league_id = get_league_id(headers, 'Premier League', 'England', 2020)
    fixture_ids = get_fixture_ids(league_id, headers)
    player_df = get_player_data(fixture_ids, headers)
    print(player_df)
