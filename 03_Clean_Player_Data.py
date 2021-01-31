#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Cleaning player data from the football api and and the official EPL api

"""

__author__ = 'Micah Cearns'
__contact__ = 'micahcearns@gmail.com'
__date__ = 'August 2020'

import pandas as pd
import os
from tqdm import tqdm

OUTPUT_PATH = ('/Users/MicahJackson/anaconda/Pycharm_WD/Draft_Kings_EPL_Project'
               '/Output')


def pandas_config():
    """
    Pandas configuration
    :return: Configured Pandas
    """
    options = {
        'display': {
            'max_columns': None,
            'max_colwidth': 50,
            'expand_frame_repr': False,  # Don't wrap to multiple pages
            'max_rows': 14,
            'max_seq_items': 50,  # Max length of printed sequence
            'precision': 4,
            'show_dimensions': False},  # Controls SettingWithCopyWarning
        'mode': {
            'chained_assignment': None
        }
    }

    for category, option in options.items():
        for op, value in option.items():
            pd.set_option(f'{category}.{op}', value)

    return

if __name__ == '__main__':

    # Code to test getting the last n games, in this case, the last 4 games
    os.chdir(OUTPUT_PATH)
    pandas_config()
    player_df = pd.read_csv('Player_fixture_df.csv')

    # Getting FPL data to get the chance of playing the next fixture score
    fpl_df = pd.read_csv('FPL_history.csv')
    fpl_df = fpl_df.loc[fpl_df['season_name'] == '2019/20']
    print(fpl_df.shape)  # (438, 26)
    print(fpl_df)

    # Let's first get rid of those who have not played
    fpl_df = fpl_df.loc[fpl_df['minutes'] != 0]
    print(fpl_df.shape)  # (392, 26) left to match

    # How many overlap before parsing?
    print(fpl_df.loc[fpl_df['full_name'].isin(player_df['player_name'])].shape)
    # (319, 26)

    # Cleaning up player names from both dfs
    fpl_df['parsed_full_name'] = (fpl_df['full_name']
                                  .str.normalize('NFKD')
                                  .str.encode('ascii', errors='ignore')
                                  .str.decode('utf-8')
                                  .str.replace('-', ' '))

    player_df['parsed_full_name'] = (player_df['player_name']
                                     .str.normalize('NFKD')
                                     .str.encode('ascii', errors='ignore')
                                     .str.decode('utf-8')
                                     .str.replace('-', ' '))

    # How many now after parsing?
    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .isin(player_df['parsed_full_name'])]
          .shape)  # 339 out of 392 (86.5%)

    # What are the ones that do not overlap
    # (53, 27)
    missing_fpl = (fpl_df
                   .loc[~fpl_df['parsed_full_name']
                   .isin(player_df['parsed_full_name'])])

    missing_fapi = (player_df
                    .loc[~player_df['parsed_full_name']
                    .isin(fpl_df['parsed_full_name'])])

    print(missing_fpl[['parsed_full_name', 'minutes']])

    # 29    Gabriel Teodoro Martinelli Silva    656.0
    # 37      Jose Ignacio Peleteiro Romallo    329.0
    # 45        Mahmoud Ahmed Ibrahim Hassan   1936.0
    # 48        Douglas Luiz Soares de Paulo   2604.0
    # 71                       Solomon March   1139.0
    #                                 ...      ...
    # 1953         Ederson Santana de Moraes   3071.0
    # 1967                      David de Gea   3420.0
    # 2027                  Lukasz Fabianski   2117.0
    # 2028              Roberto Jimenez Gago    686.0
    # 2039     Rui Pedro dos Santos Patricio   3420.0

    print(missing_fapi
          .groupby('parsed_full_name', as_index=False)
          .sum()[['parsed_full_name', 'minutes_played']])

    #     parsed_full_name  minutes_played
    # 0       Aaron Lennon             497
    # 1          Adam Idah             253
    # 2        Adam Masina            1848
    # 3         Adam Smith            2060
    # 4             Adrian             873
    # ..               ...             ...
    # 172           Wesley            1790
    # 173      Will Hughes            2272
    # 174   Will Smallbone             397
    # 175          Willian            2620
    # 176  ukasz Fabianski            2132

    # Looks like a lot of players with just individual names or spelling errors
    # that need to be fixed. Also, some of these names look like players that
    # have been transferred. Next, I need to go to the news column in the fpl
    # dataframe and search for any strings that contain transfer / transferred
    # etc and then drop them.
    print(fpl_df['player_news'].unique())
    print(fpl_df
          .loc[fpl_df['player_news']
          .str.contains('Joined'
                        '|Contract terminated'
                        '|Loan deal ended'
                        '|Returned') == True]
          .filter(items=['player_id', 'full_name']))

    #       player_id             full_name
    # 16       198849        Lucas Torreira
    # 28       242166      Matteo Guendouzi
    # 68        74471            Aaron Mooy
    # 305       61604           Matty James
    # 390      153682          Harry Wilson
    #          ...                   ...
    # 1686     242058            Moise Kean
    # 1758     196118        Yoshinori Muto
    # 1877      98770          Ørjan Nyland
    # 1987     107265            Angus Gunn
    # 2028      40694  Roberto Jimenez Gago

    # Let's get their names in a list
    transferred_names = (fpl_df
                         .loc[fpl_df['player_news']
                         .str.contains('Joined'
                                       '|Contract terminated'
                                       '|Loan deal ended'
                                       '|Returned') == True]
                        ['parsed_full_name']
                        .tolist())
    print(transferred_names)

    for n in transferred_names:
        drop_test = fpl_df.loc[fpl_df['full_name'].str.contains(n) == False]
    print(drop_test)

    # Has not dropped all of them. Let's try dropping by their index values
    # instead.
    trans_index = (fpl_df
                   .loc[fpl_df['player_news']
                   .str.contains('Joined'
                                 '|Contract terminated'
                                 '|Loan deal ended'
                                 '|Returned') == True]
                   .index)

    fpl_df = fpl_df.drop(trans_index, axis=0)
    print(fpl_df
          .loc[fpl_df['player_news']
          .str.contains('Joined'
                        '|Contract terminated'
                        '|Loan deal ended'
                        '|Returned') == True]
          .filter(items=['player_id', 'full_name']))
    print(fpl_df.shape)  # (359, 27)  # All transferred players are now dropped.

    # How many overlapping now?
    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .isin(player_df['parsed_full_name'])]
          .shape)  # Now 311 out of 359 (86.6%), the same percentage proportion

    # Who's left?
    missing_fpl = (fpl_df
                   .loc[~fpl_df['parsed_full_name']
                   .isin(player_df['parsed_full_name'])])

    missing_fapi = (player_df
                    .loc[~player_df['parsed_full_name']
                    .isin(fpl_df['parsed_full_name'])])

    print(missing_fpl[['parsed_full_name', 'minutes']]
          .sort_values(by='minutes', ascending=False))

    #                       parsed_full_name  minutes
    # 2039     Rui Pedro dos Santos Patricio   3420.0
    # 1967                      David de Gea   3420.0
    # 687   Joao Filipe Iria Santos Moutinho   3105.0
    # 1953         Ederson Santana de Moraes   3071.0
    # 1685            Richarlison de Andrade   3070.0
    #                                 ...      ...
    # 564                  William Smallbone    389.0
    # 37      Jose Ignacio Peleteiro Romallo    329.0
    # 698             Daniel Castelo Podence    285.0
    # 606          Gedson Carvalho Fernandes     62.0
    # 431                        Tommy Doyle     15.0

    print(missing_fapi
          .groupby('parsed_full_name', as_index=False)  # As is a long df
          .sum()[['parsed_full_name', 'minutes_played']]
          .sort_values(by='minutes_played', ascending=False))

    #            parsed_full_name  minutes_played
    # 171            Rui Patricio            3433
    # 21               Ben Foster            3432
    # 41             David De Gea            3423
    # 187                Tim Krul            3263
    # 134              Max Aarons            3263
    # ..                      ...             ...
    # 87             James Garner               8
    # 135            Max Thompson               3
    # 71   Georges Kevin N'Koudou               2
    # 7               Akin Famewo               1
    # 103           Jordan Thomas               1


    # Looks like it is mostly name errors now from the football API data. I will
    # need to fix these up manually now.

    # If we look at the fpl df above, we can see that they often use full
    # names, including middle names, whereas the fapi data does not. Now I will
    # parse out each name to its own column and then combine them all with the
    # first column to try and find combinations that match
    player_name_combos = ['name_1', 'name_2', 'name_3', 'name_4', 'name_5',
                          'name_6']
    col_ints = list(range(1, 7, 1))
    for name, col in zip(player_name_combos, col_ints):
        fpl_df[name] = (
                fpl_df['parsed_full_name']
                .str.split(expand=True)[0]
                .astype(str)
                + ' '
                + fpl_df['parsed_full_name']
                .str.split(expand=True)[col]
                .astype(str)
        )

    # How many overlap with these new columns that we've created
    for name in player_name_combos:
        print(name, ':')
        print(fpl_df
              .loc[fpl_df[name]
              .isin(player_df['parsed_full_name'])]
              .shape)
        print('')

    # name_1 :
    # (295, 33)
    # name_2 :
    # (5, 33)
    # name_3 :
    # (6, 33)
    # name_4 :
    # (3, 33)
    # name_5 :
    # (0, 33)
    # name_6 :
    # (1, 33)

    # Appending each df of overlapping players to a list
    name_df_list = []
    for name in player_name_combos:
        overlapping_df = (fpl_df
                          .loc[fpl_df[name]
                          .isin(player_df['parsed_full_name'])])
        name_df_list.append(overlapping_df)

    # Inspecting as dataframes
    df_list_length = len(name_df_list)
    n_dfs = range(1, df_list_length, 1)
    for n in n_dfs:
        print(pd.DataFrame(name_df_list[n]))

    # Using boolean indexing to recover the players with different names
    overlapping_player_names_df = (
        player_df
          .loc[(player_df['parsed_full_name'].isin(fpl_df['name_2']))
               | (player_df['parsed_full_name'].isin(fpl_df['name_3']))
               | (player_df['parsed_full_name'].isin(fpl_df['name_4']))
               | (player_df['parsed_full_name'].isin(fpl_df['name_5']))
               | (player_df['parsed_full_name'].isin(fpl_df['name_6']))]
          .groupby('parsed_full_name', as_index=False)
          .nth(1)
          .filter(items=['parsed_full_name'])
          .squeeze()  # To pandas series
    )  # 14 players found

    # Full and last names as lists
    overlapping_player_names_list = overlapping_player_names_df.tolist()
    overlapping_last_player_names_list = (
        overlapping_player_names_df
            .str.split(expand=True)[1]  # Grabbing the last name
            .tolist()
    )

    # These are the player_df (fapi) names that overlap but are different in the
    # fpl df. Let's find what they are called in the fpl df so we can change
    # them.
    for last_name, full_name in zip(overlapping_last_player_names_list,
                                    overlapping_player_names_list):
        print('Name in the FAPI df:')
        print(full_name)
        print('')
        print('Overlapping last names in the FPL df:')
        print(fpl_df
              .loc[fpl_df
              .parsed_full_name
              .str.contains(last_name) == True]
              .filter(items=['parsed_full_name']))
        print('')
        print('='*50)
        print('')

    # Replacing names in the FPL df to match the player df (fapi)
    to_replace = [
        'Andre Filipe Tavares Gomes',
        'Gabriel Fernando de Jesus'
        'Lucas Rodrigues Moura da Silva',
        'Ricardo Domingos Barbosa Pereira',
        'Rui Pedro dos Santos Patricio',
        'Ruben Diogo da Silva Neves'
        'Joao Filipe Iria Santos Moutinho',
        'Bernardo Mota Veiga de Carvalho e Silva',
        'Pedro Lomba Neto',
        'Joao Pedro Cavaco Cancelo',
        'Gabriel Teodoro Martinelli Silva',
        'Ruben Goncalo Silva Nascimento Vinagre',
        'Gedson Carvalho Fernandes',
        'Daniel Castelo Podence',
        'Bruno Miguel Borges Fernandes'
    ]

    replace_with = [
        'Andre Gomes',
        'Gabriel Jesus',
        'Lucas Moura',
        'Ricardo Pereira',
        'Rui Patricio',
        'Ruben Neves',
        'Joao Moutinho',
        'Bernardo Silva',
        'Pedro Neto',
        'Joao Cancelo',
        'Gabriel Martinelli',
        'Ruben Vinagre',
        'Gedson Fernandes',
        'Daniel Podence',
        'Bruno Fernandes'
    ]

    for old_name, new_name in zip(to_replace, replace_with):
        fpl_df['parsed_full_name'] = (
            fpl_df['parsed_full_name']
                .replace({old_name: new_name})
        )

    # How many overlapping now?
    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .isin(player_df['parsed_full_name'])]
          .shape) # 10 more recovered

    # How many are not overlapping now?
    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .isin(player_df['parsed_full_name']) == False]
          .shape) # 38

    # What are their names in the fpl df?
    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .isin(player_df['parsed_full_name']) == False]
          .filter(items=['parsed_full_name']))

    #                     parsed_full_name
    # 37    Jose Ignacio Peleteiro Romallo
    # 45      Mahmoud Ahmed Ibrahim Hassan
    # 48      Douglas Luiz Soares de Paulo
    # 71                     Solomon March
    # 102          Johann Berg Gudmundsson
    #                               ...
    # 1944  Adrian San Miguel del Castillo
    # 1946           Alisson Ramses Becker
    # 1953       Ederson Santana de Moraes
    # 1967                    David de Gea
    # 2027                Lukasz Fabianski

    # Next, go through the single names from the player df and come up with some
    # logic to match them with the correct names in the FPL df. For example,
    # must contain the one name from the fapi player name column as well as
    # overlap on goals, assists, or saves.

    # First I need to get the names of players that only have a single name in
    # fapi
    player_df_single_names = (
        player_df
            .loc[player_df['parsed_full_name']
            .isin(fpl_df['parsed_full_name']) == False]
            .groupby('parsed_full_name')
            .tail(1)['parsed_full_name']
            .str.split(expand=True)
    )

    # Renaming the columns
    player_df_single_names = (
        player_df_single_names
            .rename(columns={0: 'First_Name',
                             1: 'Second_Name',
                             2: 'Third_Name'})
    )

    # Filtering out those who do not have a second or third name and cleaning
    player_df_single_names = (
        player_df_single_names
            .loc[player_df_single_names['Second_Name']
            .isna() == True]
            .sort_values(by='First_Name')
            .filter(items=['First_Name'])
    )
    print(player_df_single_names)
    #       First_Name
    # 7626      Adrian
    # 10540    Alisson
    # 4245    Angelino
    # 10445    Bernard
    # 10367   Bernardo
    #           ...
    # 10499      Rodri
    # 7681    Sokratis
    # 10605  Trezeguet
    # 5524      Wesley
    # 10306    Willian

    # Converting to a list to loop through
    player_single_names_list = (player_df_single_names
                                .First_Name
                                .tolist())
    print(player_single_names_list)

    # Now searching out the individual names from fapi in the FPL df
    for name in player_single_names_list:
        print('Name in the FAPI df:')
        print(name)
        print('')
        print('Overlapping names in the FPL df:')
        print(fpl_df
              .loc[fpl_df['parsed_full_name']
              .str.contains(name) == True]
              .filter(items=['parsed_full_name', 'goals_scored', 'assists']))
        print('')
        print('='*50)
        print('')

    # Non-overlapping players
    # Trezeguet
    # Jorginho
    # Fernandinho
    # Fabinho
    # Angelino

    # Checking the individual names against the full names in the FPL df and
    # seeing finding which names have overlapping goals and assists so that I
    # can be sure that they are the correct one. If so I am correcting their
    # names in both dfs.
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Willian', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Willian': 'Willian Silva'})
    )

    # Replacing in each df
    fpl_df['parsed_full_name'] = (
        fpl_df['parsed_full_name']
            .replace({'Willian Borges Da Silva': 'Willian Silva'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str.contains('Willian')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str.contains('Willian')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Wesley', axis=0))

    print('')

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Wesley', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Wesley': 'Wesley Moraes'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str.contains('Wesley')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str.contains('Wesley')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Sokratis', axis=0))

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Sokratis', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Sokratis': 'Sokratis Papastathopoulos'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str
          .contains('Sokratis')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str
          .contains('Sokratis')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Rodri', axis=0))

    #                   goals.total  goals.assists
    # parsed_full_name
    # Jay Rodriguez               8              1
    # Rodri                       3              2

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Rodri', axis=0))

    # It is Rodrigo Hernandez when we look at overlapping goals and assists
    #                                      goals_scored  assists
    # parsed_full_name
    # Frederico Rodrigues de Paula Santos           0.0      0.0
    # Jay Rodriguez                                 8.0      2.0
    # Lucas Rodrigues Moura da Silva                4.0      5.0
    # Rodrigo Hernandez                             3.0      2.0


    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Rodri': 'Rodrigo Hernandez'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str
          .contains('Rodri')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str
          .contains('Rodri')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Richarlison', axis=0))

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Richarlison', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Richarlison': 'Richarlison Andrade'})
    )

    fpl_df['parsed_full_name'] = (
        fpl_df['parsed_full_name']
            .replace({'Richarlison de Andrade': 'Richarlison Andrade'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str
          .contains('Richarlison')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str
          .contains('Richarlison')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Pedro', axis=0))

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Pedro', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Pedro': 'Pedro Ledesma'})
    )

    fpl_df['parsed_full_name'] = (
        fpl_df['parsed_full_name']
            .replace({'Pedro Rodriguez Ledesma': 'Pedro Ledesma'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str
          .contains('Pedro')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str
          .contains('Pedro')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Jota', axis=0))

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Jota', axis=0))

    # Mmmmmm Diogo Jota is already in both dfs. I will leave this for now. Could
    # be a transferred player?

    # ========================================================================

    # Checking that it is the correct player
    print(player_df
          .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Pedro', axis=0))

    print(fpl_df
          .filter(items=['parsed_full_name', 'goals_scored', 'assists'])
          .groupby('parsed_full_name')
          .sum()
          .filter(like='Pedro', axis=0))

    # Replacing in each df
    player_df['parsed_full_name'] = (
        player_df['parsed_full_name']
            .replace({'Pedro': 'Pedro Ledesma'})
    )

    fpl_df['parsed_full_name'] = (
        fpl_df['parsed_full_name']
            .replace({'Pedro Rodriguez Ledesma': 'Pedro Ledesma'})
    )

    # Checking that it is parsed correctly in both dataframes
    print(player_df
          .loc[player_df['parsed_full_name']
          .str
          .contains('Pedro')]
          .filter(items=['parsed_full_name',
                         'player_name',
                         'goals.total',
                         'goals.assists'])
          .groupby('parsed_full_name')
          .sum())

    print('')

    print(fpl_df
          .loc[fpl_df['parsed_full_name']
          .str
          .contains('Pedro')]
          .filter(items=['parsed_full_name',
                         'full_name',
                         'goals_scored',
                         'assists']))

