import pandas as pd
import numpy as np
import os
import time

# Timer to track how long the script takes to run
start_time = time.time()

# Directory containing the raw data files
raw_data_dir = 'C:/UCLA Clubs/Bruin Sports Analytics/2024 Fall Research/Data Cleaning/Raw Data'

# Load the reference files for team and player IDs, which are in the same folder as the script
team_ids_file_path = 'NBA_Team_IDs.csv'
player_ids_file_path = 'NBA_Player_IDs.csv'

team_ids = pd.read_csv(team_ids_file_path)
player_ids = pd.read_csv(player_ids_file_path)

# Clean up the team and player IDs
# Drop duplicate entries in the team_ids and player_ids datasets
team_ids_cleaned = team_ids.drop_duplicates(subset=['NBA_Current_Link_ID'])
player_ids_cleaned = player_ids.drop_duplicates(subset=['NBAID'])

# Process each CSV file in the raw data folder
for raw_data_file in os.listdir(raw_data_dir):
    if raw_data_file.endswith('.csv'):
        # Load the dataset
        raw_data_file_path = os.path.join(raw_data_dir, raw_data_file)
        data = pd.read_csv(raw_data_file_path)
        
        # Extract the year from the file name (e.g., '2023-24')
        season_year = raw_data_file.split('_')[0].split('-')[-1]  # Extracts '24' from '2023-24'
        year = '20' + season_year  # Converts '24' to '2024'

        # Merge the shot data with team names based on 'team_id' and 'opponent_team_id'
        data = data.merge(team_ids_cleaned, left_on='team_id', right_on='NBA_Current_Link_ID', how='left')
        data = data.merge(team_ids_cleaned, left_on='opponent_team_id', right_on='NBA_Current_Link_ID', suffixes=('_team', '_opponent'), how='left')

        # Merge the shot data with player names based on 'player_id'
        data = data.merge(player_ids_cleaned, left_on='player_id', right_on='NBAID', how='left')

        # Drop unnecessary columns and rename the merged columns
        data_cleaned = data.drop(columns=['team_id', 'opponent_team_id', 'NBA_Current_Link_ID_team', 'NBA_Current_Link_ID_opponent', 'NBAID'])
        data_cleaned = data_cleaned.rename(columns={
            'Team Name_team': 'team_name',
            'Team Name_opponent': 'opponent_team_name',
            'NBAName': 'player_name'
        })

        # Reorder the columns so that team_name, opponent_team_name, and player_name are first
        cols = ['team_name', 'opponent_team_name', 'player_name', 'player_id', 'game_id'] + [col for col in data_cleaned.columns if col not in ['team_name', 'opponent_team_name', 'player_name', 'player_id', 'game_id']]
        data_cleaned = data_cleaned[cols]

        # Sort the data by game_id and then by period (smallest to largest)
        data_cleaned = data_cleaned.sort_values(by=['game_id', 'period'])

        # Rename the columns based on the new names provided
        data_cleaned_renamed = data_cleaned.rename(columns={
            'team_name': 'Team',
            'opponent_team_name': 'Opponent',
            'player_name': 'Player',
            'player_id': 'Player ID',
            'game_id': 'Game ID',
            'period': 'Quarter',
            'u10_ft_fg2m': 'Under 10 ft 2 Pt Makes',
            'u10_ft_fg2a': 'Under 10 ft 2 Pt Attempts',
            'o10_ft_fg2m': 'Over 10 ft 2 Pt Makes',
            'o10_ft_fg2a': 'Over 10 ft 2 Pt Attempts',
            'fg3m': '3 Pt Makes',
            'fg3a': '3 Pt Attempts',
            'close_def_dist': 'Closest Defender Distance',
            'shot_clock': 'Shot Clock Remaining',
            'touch_time': 'Touch Time',
            'dribble_range': 'Dribbles'
        })

        # Save the cleaned and renamed data to the "Clean Data" folder with dynamic naming
        clean_data_dir = './Clean Quarter By Quarter Data'
        os.makedirs(clean_data_dir, exist_ok=True)

        # Dynamic file naming for the Quarter-By-Quarter dataset
        quarter_by_quarter_file_name = f'{year} Quarter-By-Quarter.csv'
        output_file_path = os.path.join(clean_data_dir, quarter_by_quarter_file_name)
        data_cleaned_renamed.to_csv(output_file_path, index=False)

        print(f"Cleaned Quarter-By-Quarter data saved to {output_file_path}")

        data = data_cleaned_renamed

        # Function to generate the expanded rows for each shot type
        def generate_attempt_rows(df, shot_type, makes_col, attempts_col):
            # Create a base DataFrame for this shot type
            base_df = df.loc[df[attempts_col] > 0].copy()

            # Generate 'Make' rows
            make_rows = base_df.loc[base_df[makes_col] > 0].copy()
            make_rows['Result'] = 'Make'
            make_rows['Attempt Type'] = shot_type
            make_rows = make_rows.loc[make_rows.index.repeat(make_rows[makes_col])]

            # Generate 'Miss' rows
            miss_rows = base_df.copy()
            miss_rows['Misses'] = base_df[attempts_col] - base_df[makes_col]
            miss_rows = miss_rows.loc[miss_rows['Misses'] > 0].copy()
            miss_rows['Result'] = 'Miss'
            miss_rows['Attempt Type'] = shot_type
            miss_rows = miss_rows.loc[miss_rows.index.repeat(miss_rows['Misses'])]

            # Combine both 'Make' and 'Miss' rows
            return pd.concat([make_rows, miss_rows])

        # Apply the function for each shot type
        under_10_ft_2pt = generate_attempt_rows(data, 'Under 10 ft 2 Pt', 'Under 10 ft 2 Pt Makes', 'Under 10 ft 2 Pt Attempts')
        over_10_ft_2pt = generate_attempt_rows(data, 'Over 10 ft 2 Pt', 'Over 10 ft 2 Pt Makes', 'Over 10 ft 2 Pt Attempts')
        three_pt = generate_attempt_rows(data, '3 Pt', '3 Pt Makes', '3 Pt Attempts')

        # Combine all expanded rows
        expanded_data = pd.concat([under_10_ft_2pt, over_10_ft_2pt, three_pt])

        # Drop unnecessary columns
        expanded_data_cleaned = expanded_data.drop(columns=[
            'Under 10 ft 2 Pt Makes',
            'Under 10 ft 2 Pt Attempts',
            'Over 10 ft 2 Pt Makes',
            'Over 10 ft 2 Pt Attempts',
            '3 Pt Makes',
            '3 Pt Attempts',
            'Misses'
        ])

        # Reorder the columns so that 'Attempt Type' is second-to-last and 'Result' is the last column
        cols = [col for col in expanded_data_cleaned.columns if col not in ['Attempt Type', 'Result']] + ['Attempt Type', 'Result']
        expanded_data_cleaned = expanded_data_cleaned[cols]

        # Create the "Clean Data" directory and save the file with dynamic naming
        clean_data_dir = './Clean Play-by-Play Data'
        os.makedirs(clean_data_dir, exist_ok=True)

        # Dynamic file naming for the Play-By-Play dataset
        play_by_play_file_name = f'{year} Play-By-Play.csv'
        output_file_path = os.path.join(clean_data_dir, play_by_play_file_name)
        expanded_data_cleaned.to_csv(output_file_path, index=False)

        print(f"Cleaned Play-By-Play data saved to {output_file_path}")

# End timer and display the total time taken
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Script finished running in {elapsed_time:.2f} seconds.")
