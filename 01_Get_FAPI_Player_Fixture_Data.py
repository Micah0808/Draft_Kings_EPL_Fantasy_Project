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
        'x-rapidapi-key': "5759a8f463mshc61ad720c308949p1a3d99jsne932e0026d6b"
    }
    return headers


def get_league_id(headers,
                  league='Premier League',
                  country='England',
                  year=2020):
    """
    Get league id to identify fixtures and players
    :param league:
    :param country:
    :param year:
    :return:
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

    :param league_id:
    :param headers:
    :return:
    """
    epl_url = ('https://api-football-v1.p.rapidapi.com/v2/fixtures/league/'
               + league_id.astype(str))
    response_epl = requests.request('GET', epl_url, headers=headers)
    epl_fixture_id_df = pd.json_normalize(response_epl.json()['api']['fixtures'])
    fixture_ids = epl_fixture_id_df['fixture_id'].astype(str).tolist()

    return fixture_ids


def get_player_data(fixture_ids, headers):
    """
    Get player data by fixture
    :param fixture_ids:
    :param headers:
    :return:
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
    headers = connect_to_api()  # League id is 524
    league_id = get_league_id(headers, 'Premier League', 'England', 2020)
    fixture_ids = get_fixture_ids(league_id, headers)
    player_df = get_player_data(fixture_ids, headers)
    print(player_df)