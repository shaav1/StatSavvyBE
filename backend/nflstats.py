import nfl_data_py as nfl
import pandas as pd
import re
import firebase as fb

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# qbs
pfr_data = nfl.import_seasonal_pfr('pass', [2024])
ngs_data = nfl.import_ngs_data('passing', [2024])
pfr_columns = ['player', 'pfr_id']
# receiving
rec_pfr_data = nfl.import_seasonal_pfr('rec', [2024])[pfr_columns]
rec_ngs_data = nfl.import_ngs_data('receiving', [2024]).rename(
    columns={'player_display_name': 'player'}).drop_duplicates().sort_values('player')

# rushing
rush_pfr_data = nfl.import_seasonal_pfr('rush', [2024])
rush_ngs_data = nfl.import_ngs_data('rushing', [2024]).rename(
    columns={'player_display_name': 'player'}).drop_duplicates().sort_values('player')


def clean_name(name):
    return re.sub('[^A-Za-z0-9]+', '', name).lower().replace(' ', '')


# qb
pfr_data['clean_name'] = pfr_data['player'].apply(clean_name)
ngs_data['clean_name'] = ngs_data['player_display_name'].apply(clean_name)
# rec
rec_pfr_data['clean_name'] = rec_pfr_data['player'].apply(clean_name)
rec_ngs_data['clean_name'] = rec_ngs_data['player'].apply(clean_name)
# rush
rush_pfr_data['clean_name'] = rush_pfr_data['player'].apply(clean_name)
rush_ngs_data['clean_name'] = rush_ngs_data['player'].apply(clean_name)

rec_ngs_columns_to_average = [
    'avg_time_to_throw', 'avg_completed_air_yards', 'avg_intended_air_yards',
    'avg_air_yards_differential', 'aggressiveness', 'max_completed_air_distance',
    'avg_air_yards_to_sticks', 'attempts', 'pass_yards', 'pass_touchdowns', 'interceptions', 'passer_rating',
    'completions', 'completion_percentage', 'expected_completion_percentage',
    'completion_percentage_above_expectation', 'avg_air_distance', 'max_air_distance'
]
ngs_data = ngs_data[ngs_data['week'] != 0]

ngs_aggregated = ngs_data.groupby('clean_name')[rec_ngs_columns_to_average].mean().reset_index()

combined_data = pd.merge(pfr_data, ngs_aggregated, on='clean_name', suffixes=('_pfr', '_ngs'))

relevant_columns = ['player', 'pfr_id', 'team', 'pass_attempts', 'throwaways', 'spikes',
                    'drops', 'drop_pct', 'bad_throws', 'bad_throw_pct', 'pocket_time', 'times_blitzed',
                    'times_hurried', 'times_hit', 'times_pressured', 'pressure_pct', 'batted_balls', 'on_tgt_throws',
                    'on_tgt_pct', 'rpo_plays', 'rpo_yards', 'rpo_pass_att',
                    'rpo_pass_yards', 'rpo_rush_att', 'rpo_rush_yards', 'pa_pass_att', 'pa_pass_yards',
                    'avg_time_to_throw', 'avg_completed_air_yards',
                    'avg_intended_air_yards', 'avg_air_yards_differential',
                    'aggressiveness', 'max_completed_air_distance', 'avg_air_yards_to_sticks', 'attempts', 'pass_yards',
                    'pass_touchdowns', 'interceptions', 'passer_rating',
                    'completions', 'completion_percentage', 'expected_completion_percentage',
                    'completion_percentage_above_expectation', 'avg_air_distance', 'max_air_distance',

                    ]
final_data = combined_data[relevant_columns]

######################## RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving RECeiving

rec_ngs_data = rec_ngs_data[rec_ngs_data['week'] != 0]  # not include week 0, preseason

rec_ngs_columns_to_average = [  # columns to avg
    'avg_cushion', 'avg_separation', 'avg_intended_air_yards',
    'percent_share_of_intended_air_yards', 'receptions',
    'targets', 'catch_percentage', 'yards', 'avg_yac', 'avg_expected_yac', 'avg_yac_above_expectation'
]

rec_ngs_aggregated = rec_ngs_data.groupby('clean_name')[rec_ngs_columns_to_average].mean().reset_index()

# Merge the datasets
rec_combined_data = pd.merge(
    rec_pfr_data,
    rec_ngs_aggregated,  # select only player column from NGS data
    on='clean_name',
    how='inner'
)
'''for index, rows in rec_combined_data.iterrows():
    player_data = {
        'avg_cushion': rows['avg_cushion'],
        'avg_separation': rows['avg_separation'],
        'avg_intended_air_yards': rows['avg_intended_air_yards'],
        'percent_share_of_intended_air_yards': rows['percent_share_of_intended_air_yards'],
        'avg_receptions': rows['receptions'],
        'avg_targets': rows['targets'],
        'catch_percentage': rows['catch_percentage'],
        'avg_receiving_yards': rows['yards'],
        'avg_yac': rows['avg_yac'],
        'avg_expected_yac': rows['avg_expected_yac'],
        'avg_yac_above_expectation': rows['avg_yac_above_expectation']
    }
    fb.update_player(rows['pfr_id'], player_data)'''

###############################  rushing rushing rushing rushing rushing rushing rushing rushing rushing rushing rushing rushing rushing rushing
rush_ngs_data = rush_ngs_data[rush_ngs_data['week'] != 0]

rush_ngs_columns_to_average = [  # columns to avg
    'efficiency', 'percent_attempts_gte_eight_defenders', 'avg_time_to_los',
    'rush_attempts', 'rush_yards',
    'avg_rush_yards'
]

rush_ngs_aggregated = rush_ngs_data.groupby('clean_name')[rush_ngs_columns_to_average].mean().reset_index()

'''for name, position, pfr_id in zip(rec_combined_data['player_x'],
                                 rec_combined_data['player_position'],
                                 rec_combined_data['pfr_id']):
    fb.add_player(pfr_id,name,position)'''

rush_combined_data = pd.merge(
    rush_pfr_data,
    rush_ngs_aggregated,  # select only player column from NGS data
    on='clean_name',
    how='inner'
)

'''for index, rows in rush_combined_data.iterrows():
    player_data = {
        'efficiency_per_game': rows['efficiency'],
        'percent_attempts_gte_eight_defenders': rows['percent_attempts_gte_eight_defenders'],
        'avg_time_to_los': rows['avg_time_to_los'],
        'avg_rush_attempts': rows['rush_attempts'],
        'rush_yards_per_game': rows['rush_yards'],
        'avg_rush_yards_per_game': rows['avg_rush_yards'],
    }
    fb.update_player(rows['pfr_id'], player_data)'''

'''for name, position, pfr_id in zip(rush_combined_data['player_x'],
                                  rush_combined_data['player_position'],
                                  rush_combined_data['pfr_id']):
    fb.add_player(pfr_id, name, position)'''

