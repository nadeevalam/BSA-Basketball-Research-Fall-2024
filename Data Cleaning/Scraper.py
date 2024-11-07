import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import threading
from requests.exceptions import ConnectionError, ReadTimeout

# Path to the folder containing Excel files
folder_path = "C:/UCLA Clubs/Bruin Sports Analytics/2024 Fall Research/Data Cleaning/Clean Play-by-Play Data"

# Initialize a global flag to stop the timer
stop_timer = False

# Timer function to display elapsed time
def display_timer(start_time):
    while not stop_timer:
        elapsed_time = time.time() - start_time
        print(f"\rElapsed Time: {elapsed_time:.2f} seconds", end="")
        time.sleep(1)  # Update every second

# Start the timer and display thread
start_time = time.time()
timer_thread = threading.Thread(target=display_timer, args=(start_time,))
timer_thread.start()

# Function to fetch player data with retry and backoff logic
def fetch_player_name(player_id):
    url = f'https://www.nba.com/stats/player/{player_id}'
    retries = 3  # Number of retries
    initial_delay = 2  # Initial delay in seconds between retries
    timeout_duration = 15  # Timeout duration in seconds
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout_duration)
            if response.status_code == 200:
                # Parse the HTML content
                soup = BeautifulSoup(response.text, 'html.parser')
                name_tags = soup.find_all('p', class_='PlayerSummary_playerNameText___MhqC')
                if name_tags and len(name_tags) >= 2:
                    # Concatenate the first and last name
                    first_name = name_tags[0].text.strip()
                    last_name = name_tags[1].text.strip()
                    return f"{first_name} {last_name}"
        except (ConnectionError, ReadTimeout) as e:
            print(f"\nError for player ID {player_id}: {e}. Retrying in {initial_delay} seconds... ({attempt + 1}/{retries})")
            time.sleep(initial_delay)  # Wait before retrying
            initial_delay *= 2  # Exponential backoff
    return None  # Return None if all attempts fail

# Step 1: Gather all unique player IDs that need names
missing_player_ids = set()  # Use a set to avoid duplicates

# Loop through each Excel file to find missing player IDs
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        
        # Load the Excel file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Find rows where "Player" is missing or empty but "Player ID" is present
        missing_players = df[(df["Player"].isna()) & (df["Player ID"].notna())]
        
        # Add player IDs to the set
        missing_player_ids.update(missing_players["Player ID"].astype(int))

# Display the number of unique missing player IDs
print(f"\nTotal missing player IDs to scrape: {len(missing_player_ids)}")

# Step 2: Scrape each unique player ID only once and store in a list
scraped_data = []  # List to hold the scraped player names and IDs

for player_id in missing_player_ids:
    player_name = fetch_player_name(player_id)
    if player_name:
        scraped_data.append({
            'Player Name': player_name,
            'Player ID': player_id
        })
        print(f"\nSuccessfully scraped {player_name} for Player ID {player_id}")
    else:
        print(f"\nFailed to retrieve player name for Player ID {player_id}")

# Step 3: Output scraped data to a new Excel file
output_df = pd.DataFrame(scraped_data)
output_df.to_excel("missing_player_names.xlsx", index=False)
print("\nMissing player names saved to missing_player_names.xlsx")

# Signal the timer thread to stop and wait for it to finish
stop_timer = True
timer_thread.join()

# Stop the main timer and display the total duration
end_time = time.time()
print(f"\nScript completed in {end_time - start_time:.2f} seconds.")
