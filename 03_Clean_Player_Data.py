#!/usr/bin/env python
# encoding: utf-8

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


# Code to test getting the last n games, in this case, the last 4 games
os.chdir(OUTPUT_PATH)
pandas_config()
player_df = pd.read_csv('Player_fixture_df.csv')

# Getting FPL data to get the chance of playing the next fixture score
fpl_df = pd.read_csv('FPL_history.csv')
fpl_df = fpl_df.loc[fpl_df['season_name'] == '2019/20']
print(fpl_df.shape)  # (666, 26)
print(fpl_df)

# Let's first get rid of those who have not played
fpl_df = fpl_df.loc[fpl_df['minutes'] != 0]
print(fpl_df.shape)  # 515 left to match

# How many overlap before parsing?
print(fpl_df.loc[fpl_df['full_name'].isin(player_df['player_name'])].shape)
# (423, 26)

# Cleaning up player names
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
      .shape)  # 448 out of 515 (87%)

# What are the ones that do not overlap
# (68, 27)
missing_fpl = (fpl_df
               .loc[~fpl_df['parsed_full_name']
               .isin(player_df['parsed_full_name'])])

missing_fapi = (player_df
                .loc[~player_df['parsed_full_name']
                .isin(fpl_df['parsed_full_name'])])

print(missing_fpl[['parsed_full_name', 'minutes']])

#                         parsed_full_name  minutes
# 24             Sokratis Papastathopoulos     1696
# 65                     Ahmed El Mohamady     1024
# 93    Bernardo Fernandes da Silva Junior      675
# 207           David Luiz Moreira Marinho     2809
# 210          Emerson Palmieri dos Santos     1022
#                                   ...      ...
# 2642           Gedson Carvalho Fernandes       62
# 2644           Bruno Andre Cavaco Jordao        6
# 2653       Bruno Miguel Borges Fernandes     1187
# 2654              Daniel Castelo Podence      285
# 2665                         Tommy Doyle       15

print(missing_fapi
      .groupby('parsed_full_name', as_index=False)
      .sum()[['parsed_full_name', 'minutes_played']])

#      parsed_full_name  minutes_played
# 0              Adrian             873
# 1   Ahmed El-Mohamady            1050
# 2             Alisson            2551
# 3         Andre Gomes            1474
# 4            Angelino             422
# ..                ...             ...
# 64          Trezeguet            1961
# 65             Wesley            1790
# 66     Will Smallbone             397
# 67            Willian            2620
# 68    ukasz Fabianski            2132

# Looks like a lot of players with just individual names or spelling errors
# that need to be fixed. Also, some of these names look like players that have
# been transferred. Next, I need to go to the news column in the fpl dataframe
# and search for any strings that contain transfer / transferred etc and
# then drop them.
print(fpl_df['player_news'].unique())
print(fpl_df.loc[fpl_df['player_news'].str.contains('Joined'
                                                    '|Contract terminated'
                                                    '|Loan deal ended'
                                                    '|Returned') == True])

#       player_id  ...            parsed_full_name
# 32        38411  ...               Nacho Monreal
# 98        42748  ...                 Gaetan Bong
# 458       18892  ...                Ashley Young
# 709       41945  ...             Sebastian Prodl
# 800      145235  ...  Jose Angel Esmoris Tasende
# 849      178876  ...        Jesus Vallejo Lazaro
# 971       54764  ...             Jonathan Kodjia
# 980      106757  ...              Jurgen Locadia
# 1113     179587  ...               Dennis Srbeny
# 1196      43020  ...   Javier Hernandez Balcazar
# 1231     209353  ...             Patrick Cutrone
# 1238     447235  ...                Troy Parrott
# 1976      42774  ...         Morgan Schneiderlin
# 2114     182156  ...                  Leroy Sane
# 2231      76542  ...               Sung-yueng Ki
# 2313      80607  ...           Christian Eriksen
# 2342      54756  ...              Victor Wanyama
# 2566     128348  ...              Ibrahim Amadou
# 2572     175946  ...             Victor Camarasa
# 2589     168566  ...       Georges-Kevin Nkoudou
# 2639     139110  ...                 Ondrej Duda

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

# Has not dropped all of them. Let's try dropping by their index values instead.
trans_index = (fpl_df
               .loc[fpl_df['player_news']
               .str.contains('Joined'
                             '|Contract terminated'
                             '|Loan deal ended'
                             '|Returned') == True]
               .index)

fpl_df = fpl_df.drop(trans_index, axis=0)
print(fpl_df.shape)  # (493, 27)
# All transferred players are now dropped.

# How many overlapping now?
print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .isin(player_df['parsed_full_name'])]
      .shape)  # Now 431 out of 493 (87%), the same percentage proportion

# Who's left?
missing_fpl = (fpl_df
               .loc[~fpl_df['parsed_full_name']
               .isin(player_df['parsed_full_name'])])

missing_fapi = (player_df
                .loc[~player_df['parsed_full_name']
                .isin(fpl_df['parsed_full_name'])])

print(missing_fpl[['parsed_full_name', 'minutes']].sort_values(by='minutes'))

#                         parsed_full_name  minutes
# 24             Sokratis Papastathopoulos     1696
# 65                     Ahmed El Mohamady     1024
# 93    Bernardo Fernandes da Silva Junior      675
# 207           David Luiz Moreira Marinho     2809
# 210          Emerson Palmieri dos Santos     1022
#                                   ...      ...
# 2642           Gedson Carvalho Fernandes       62
# 2644           Bruno Andre Cavaco Jordao        6
# 2653       Bruno Miguel Borges Fernandes     1187
# 2654              Daniel Castelo Podence      285
# 2665                         Tommy Doyle       15

print(missing_fapi
      .groupby('parsed_full_name', as_index=False)
      .sum()[['parsed_full_name', 'minutes_played']]
      .sort_values(by='minutes_played')[0:50])

#      parsed_full_name  minutes_played
# 0              Adrian             873
# 1   Ahmed El-Mohamady            1050
# 2             Alisson            2551
# 3         Andre Gomes            1474
# 4            Angelino             422
# ..                ...             ...
# 80     Victor Wanyama              24
# 81             Wesley            1790
# 82     Will Smallbone             397
# 83            Willian            2620
# 84    ukasz Fabianski            2132

# Looks like it is mostly name errors now from the football API data. I will
# need to fix these up manually now.

# If we look at the fpl df above, we can see that they often use full
# names, including middle names, whereas the fapi data does not. Now I will
# parse out each name to its own column and then combine them all with the first
# column to try and find combinations that match
player_name_combos = ['name_1', 'name_2', 'name_3', 'name_4', 'name_5', 'name_6']
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
# (419, 33)
# name_2 :
# (5, 33)
# name_3 :
# (7, 33)
# name_4 :
# (4, 33)
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

# Using boolean indexing to recover the players wtih different names
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
      .squeeze()
)  # 15 players found

# Full and last names as lists
overlapping_player_names_list = overlapping_player_names_df.tolist()
overlapping_last_player_names_list = (
    overlapping_player_names_df
        .str.split(expand=True)[1]  # Grabbing the last name
        .tolist()
)

# These are the player_df (fapi) names that overlap but are different in the
# fpl df. Let's find what they are called in the fpl df so we can change them.
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
      .shape) # 11 more recovered, not all 15? Check which ones have not?

# How many are not overlapping now?
print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .isin(player_df['parsed_full_name']) == False]
      .shape) # 67

# What are their names in the fpl df?
print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .isin(player_df['parsed_full_name']) == False]
      .filter(items=['parsed_full_name']))

# 24               Sokratis Papastathopoulos
# 93      Bernardo Fernandes da Silva Junior
# 207             David Luiz Moreira Marinho
# 210            Emerson Palmieri dos Santos
# 318       Ricardo Domingos Barbosa Pereira
#                        ...
# 2642             Gedson Carvalho Fernandes
# 2644             Bruno Andre Cavaco Jordao
# 2653         Bruno Miguel Borges Fernandes
# 2654                Daniel Castelo Podence
# 2665                           Tommy Doyle

# Next, go through the single names from the player df and come up with some
# logic to match them with the correct names in the FPL df. For example,
# must contain the one name from the fapi player name column as well as overlap
# on goals, assists, or saves.

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

# Converting to a list to loop through
player_single_names_list = (player_df_single_names
                            .First_Name
                            .tolist())
print(player_single_names_list)

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

# Checking the individual names against the full names in the FPL df and seeing
# finding which names have overlapping goals and assists so that I can be sure
# that they are the correct one. If so I am correcting their names in both dfs.
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

print('')

print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .str.contains('Willian')]
      .filter(items=['parsed_full_name',
                     'full_name',
                     'goals_scored',
                     'assists']))

# ============================================================================

# Checking that it is the correct player
print(player_df
      .filter(items=['parsed_full_name', 'goals.total', 'goals.assists'])
      .groupby('parsed_full_name')
      .sum()
      .filter(like='Wesley', axis=0))

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

# =============================================================================

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

# =============================================================================

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

# It is Rodrigo Hernandez when we look at overlapping goals and assits
#                                      goals_scored  assists
# parsed_full_name
# Frederico Rodrigues de Paula Santos             0        0
# Jay Rodriguez                                   8        2
# Lucas Rodrigues Moura da Silva                  4        5
# Pedro Rodriguez Ledesma                         1        2
# Rodrigo Hernandez                               3        2

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

# =============================================================================

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

# =============================================================================

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

# =============================================================================

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

# Mmmmmm Diogo Jota is already in both dfs. I will eave this for now. Could be
# a transferred player?

# =============================================================================

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


# Finally, use this later to replicate the DK scoring rules
# https://stackoverflow.com/questions/57064427/pandas-adding-a-scoring-column-
# based-on-multiple-criteria

# ==================================================

# Name in the FAPI df:
# Andre Gomes

# Overlapping last names in the FPL df:
# 2188                      Angel Gomes
# 2409    Felipe Anderson Pereira Gomes
# 2484       Andre Filipe Tavares Gomes

# ==================================================

# Name in the FAPI df:
# Gabriel Jesus

# Overlapping last names in the FPL df:
# 1088        Gabriel Fernando de Jesus
# 1286    Joao Pedro Junqueira de Jesus

# ==================================================

# Name in the FAPI df:
# Lucas Moura

# Overlapping last names in the FPL df:
# 2321    Lucas Rodrigues Moura da Silva

# ==================================================

# Name in the FAPI df:
# Ricardo Pereira

# Overlapping last names in the FPL df:
# 318     Ricardo Domingos Barbosa Pereira
# 2194                     Andreas Pereira
# 2409       Felipe Anderson Pereira Gomes

# ==================================================

# Name in the FAPI df:
# Rui Patricio

# Overlapping last names in the FPL df:
# 1539    Rui Pedro dos Santos Patricio

# ==================================================

# Name in the FAPI df:
# Ruben Neves

# Overlapping last names in the FPL df:
# 2467    Ruben Diogo da Silva Neves

# ==================================================

# Name in the FAPI df:
# Joao Moutinho

# Overlapping last names in the FPL df:
# 2469    Joao Filipe Iria Santos Moutinho

# ==================================================

# Name in the FAPI df:
# Bernardo Silva

# Overlapping last names in the FPL df:
# 93           Bernardo Fernandes da Silva Junior
# 779      Ruben Goncalo Silva Nascimento Vinagre
# 1237           Gabriel Teodoro Martinelli Silva
# 1826                    Willian Borges Da Silva
# 2123    Bernardo Mota Veiga de Carvalho e Silva
# 2133                                David Silva
# 2321             Lucas Rodrigues Moura da Silva
# 2467                 Ruben Diogo da Silva Neves

# ==================================================

# Name in the FAPI df:
# Pedro Neto

# Overlapping last names in the FPL df:
# 2590    Pedro Lomba Neto

# ==================================================

# Name in the FAPI df:
# Joao Cancelo

# Overlapping last names in the FPL df:
# 891    Joao Pedro Cavaco Cancelo

# ==================================================

# Name in the FAPI df:
# Gabriel Martinelli

# Overlapping last names in the FPL df:
# 1237    Gabriel Teodoro Martinelli Silva

# ==================================================

# Name in the FAPI df:
# Ruben Vinagre

# Overlapping last names in the FPL df:
# 779    Ruben Goncalo Silva Nascimento Vinagre

# ==================================================

# Name in the FAPI df:
# Gedson Fernandes

# Overlapping last names in the FPL df:
# 93      Bernardo Fernandes da Silva Junior
# 2642             Gedson Carvalho Fernandes
# 2653         Bruno Miguel Borges Fernandes

# ==================================================

# Name in the FAPI df:
# Daniel Podence

# Overlapping last names in the FPL df:
# 2654    Daniel Castelo Podence

# ==================================================

# Name in the FAPI df:
# Bruno Fernandes

# Overlapping last names in the FPL df:
# 93      Bernardo Fernandes da Silva Junior
# 2642             Gedson Carvalho Fernandes
# 2653         Bruno Miguel Borges Fernandes

# ==================================================







# print(player_df
#       .loc[(player_df['parsed_full_name'].isin(fpl_df['parsed_full_name']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_1']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_2']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_3']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_4']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_5']))
#            | (player_df['parsed_full_name'].isin(fpl_df['name_6']))]
#       .shape)  # (9642, 41)













# # Getting the names of the non-overlapping names that have actually played this
# # season
# print(missing_df['minutes'].unique())
# print(missing_df.loc[missing_df['minutes'] != 0].shape)
# missing_df = missing_df.loc[missing_df['minutes'] != 0]
# print(missing_df.shape)  # (68, 27)
# print(missing_df[ 'parsed_full_name'])

# Seems to be those with long European names that are not overlapping, need to
# manually check these and maybe remove the middle names.
# 24               Sokratis Papastathopoulos
# 65                       Ahmed El Mohamady
# 93      Bernardo Fernandes da Silva Junior
# 207             David Luiz Moreira Marinho
# 210            Emerson Palmieri dos Santos
#                        ...
# 2642             Gedson Carvalho Fernandes
# 2644             Bruno Andre Cavaco Jordao
# 2653         Bruno Miguel Borges Fernandes
# 2654                Daniel Castelo Podence
# 2665                           Tommy Doyle

# let's inspect some examples
print(player_df
      .loc[player_df['parsed_full_name']
      .str.contains('Mohamady') == True])  # Ahmed El-Mohamady

# Let's now remove these: -
player_df['parsed_full_name'] = (player_df['player_name'].str.replace('-', ' '))

# Now they match
print(player_df
      .loc[player_df['parsed_full_name']
      .str.contains('Mohamady') == True]['parsed_full_name']
      .tolist())  # Ahmed El Mohamady

print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .str.contains('Mohamady') == True]['parsed_full_name']
      .tolist())  # Ahmed El Mohamady

# Let's see how many match now
print(fpl_df
      .loc[fpl_df['parsed_full_name']
      .isin(player_df['parsed_full_name'])].shape)  # (367, 27)

# Less match




# Cleaning up and parsing player ratings
player_df = (player_df
             .loc[player_df['rating']
             .str
             .contains('–') == False])

print(player_df['rating'].unique())
print(player_df.shape)

# Getting player ratings for the last x games
avg_player_ratings_dfs = []
n_games_list = [2, 3, 4, 5]
for n in n_games_list:
    ratings_dfs = (player_df
                   .groupby('player_id')
                   .tail(n)  # Last 4 games played by each player
                   .assign(rating=lambda x: x.rating.astype(float))
                   .groupby('player_name', as_index=False)
                   .mean()  # Average rating over the last 4 games
                   .sort_values(by='rating', ascending=False))
    avg_player_ratings_dfs.append(ratings_dfs)

print(player_df.shape)
# (10410, 40)

print(player_df.loc[player_df['substitute'] == False].shape)
#  (9081, 40)

print(player_df['minutes_played']
      .value_counts()
      .reset_index()
      .sort_values(by='index', ascending=False)[0:10])

#     index  minutes_played
# 76    102              11
# 73     99              14
# 9      97              93
# 43     96              55
# 36     95              59
# 32     94              61
# 74     93              14
# 69     92              19
# 68     91              20
# 0      90            5735

print(player_df.loc[player_df['minutes_played'] >= 70].shape)
# (7498, 40)

# Next, need to parse rating to a float, groupby player_name, and take the
# average rating. Rating needs to be cleaned first. This should all be done in
# the next script that will be devoted to cleaning and parsing.