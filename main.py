import pandas as pd
import os
import requests
import time
import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')

#------------------------------------Generic functions

def make_request(url, retry=4, wait=2):
    """
    Make a GET request to the specified URL with retry logic.
    
    Parameters:
    - url (str): The URL to make the request to.
    - retry (int): Number of retries in case of failure.
    - wait (int): Seconds to wait before retrying.
    
    Returns:
    - response (requests.Response): The response object from the request.
    """
    for attempt in range(retry):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                print(f'Rate limit exceeded. Waiting for {wait} seconds before retrying...')
                time.sleep(wait)
            else:
                print(f'Error fetching data: {response.status_code}')
                break
        except requests.exceptions.RequestException as e:
            print(f'Error connecting to API: {e}')
    
    print(f'[make_resquest] failed to connect to API')
    return None

#------------------------------------user identification

def get_user_id_by_vanity(api_key):
    """
    Get Steam user ID by vanity URL.
    
    Parameters:
    - api_key (str): The API key for Steam API.
    
    Returns:
    - steam_user_id (str): The Steam user ID.
    """

    print("Welcome to the Steam Data Extractor!")
    print("This tool allows you to extract Steam user data and game achievements.\n")
    print("Please select how you want to identify the user:")

    while True:
        try:
            input_option = int(input('Select an option:\n1. By Steam ID\n2. By Vanity URL\n'))
            
            if input_option in(1, 2):
                break
            else:
                print('Invalid option. Please select 1 or 2.')
        except ValueError:
            print('Invalid input. Please enter a number.')

    if input_option == 1:
        steam_user_id = input('Enter the Steam ID (ex 7656119...): ')
        print(f'You selected to identify by Steam ID: {steam_user_id}')
        return steam_user_id
    elif input_option == 2:
        vanity_name = input('Please enter the name present in your profile URL: ')
        vanity_url = f'https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/?key={api_key}&vanityurl={vanity_name}'

        response = make_request(vanity_url, retry=4, wait=2)

        if response is not None and response.status_code == 200:
            json_vanity_info = response.json()
            if json_vanity_info.get('response').get('success') == 1:
                steam_user_id = json_vanity_info.get('response').get('steamid')
                return steam_user_id
        else:
            print(f'Error: Vanity URL not found or does not exist. Please check the vanity name: {vanity_name} or use your Steam ID.')
            return None
        
        
steam_user_id = get_user_id_by_vanity(API_KEY)

#------------------------------------Get all user informations by steam id

def get_user_info(steam_user_id, api_key):
    """
    Get user information from Steam API by user ID.
    
    Parameters:
    - steam_user_id (str): The Steam user ID.
    - api_key (str): The API key for Steam API.
    
    Returns:
    - json_user_info (dict): The JSON response containing user information.
    """
    
    url = f'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={api_key}&steamids={steam_user_id}'
    response = make_request(url, retry=4, wait=2)

    if response is not None and response.status_code == 200:
        json_user_info = response.json()
        return json_user_info
    else:
        print(f'Error fetching user information for Steam ID {steam_user_id}. Please check the ID or your API key.')
        return None


players_info = get_user_info(steam_user_id, API_KEY).get('response').get('players')[0]
df_steam_user = pd.DataFrame([players_info])


df_user_final = pd.DataFrame(
    {
    'steamid': df_steam_user['steamid'],
    'communityvisibilitystate': df_steam_user['communityvisibilitystate'],
    'profilestate': df_steam_user['profilestate'],
    'avatarhash': df_steam_user['avatarhash'],
    'personaname': df_steam_user['personaname'],
    'profileurl': df_steam_user['profileurl'],
    'timecreated': df_steam_user['timecreated'].apply(lambda x: datetime.fromtimestamp(x)) if 'timecreated' in df_steam_user.columns else None,
    'lastlogoff': df_steam_user['lastlogoff'].apply(lambda x : datetime.fromtimestamp(x)) if 'lastlogoff' in df_steam_user.columns else None,
    'loccountrycode': df_steam_user['loccountrycode'],
    'avatarmedium': df_steam_user['avatarmedium']
    }
)

if df_user_final['communityvisibilitystate'].iloc[0] != 3:
    print(f'Hello {df_user_final['personaname']} profile is private, cannot fetch games. Please make it public to continue.')
    exit()

#------------------------------------Get list of owned games

def get_owned_games(steam_user_id, api_key):
    """
    Get the list of owned games by a Steam user.
    
    Parameters:
    - steam_user_id (str): The Steam user ID.
    - api_key (str): The API key for Steam API.
    
    Returns:
    - json_games_info (dict): The JSON response containing game information.
    """
    
    games_info_url = f'https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={steam_user_id}&include_appinfo=true&include_played_free_games=true&format=json'
    
    response = make_request(games_info_url, retry=4, wait=2)

    if response is not None and response.status_code == 200:
        json_games_info = response.json()
        return json_games_info
    else:
        print(f'Error fetching owned games for Steam ID {steam_user_id}. Please check the ID or your API key.')
        return None


games_info = get_owned_games(steam_user_id, API_KEY).get('response').get('games')

game_list = []

for game in enumerate(games_info):
    game_datails = game[1]

    game_list.append(game_datails)

df_game_information = pd.DataFrame(game_list)

df_game_information['steam_player_id'] = steam_user_id
df_game_information_filtered = df_game_information[df_game_information['playtime_forever'] > 0]

df_game_information_final = pd.DataFrame(
    {
        'steam_user_id': df_game_information_filtered['steam_player_id'],
        'steam_game_id': df_game_information_filtered['appid'],
        'name': df_game_information_filtered['name'],
        'last_played_timestamp': df_game_information_filtered['rtime_last_played'].apply(lambda x: datetime.fromtimestamp(x)) if 'rtime_last_played' in df_game_information_filtered.columns else None,
        'playtime_forever': df_game_information_filtered['playtime_forever'],
        'img_icon_url': df_game_information_filtered['img_icon_url'],
        'has_community_visible_stats': df_game_information_filtered['has_community_visible_stats'],
        'playtime_2weeks': df_game_information_filtered['playtime_2weeks'] if 'playtime_2weeks' in df_game_information_filtered.columns else None,

    } 
)

#------------------------------------#Get list of game achievements

collect_achievements_info = []
no_information_games = []

def get_game_achievements(steam_user_id, api_key, appid):
    """
    Get the achievements for a specific game by appid.
    
    Parameters:
    - steam_user_id (str): The Steam user ID.
    - api_key (str): The API key for Steam API.
    - appid (int): The application ID of the game.
    
    Returns:
    - json_game_achievements (dict): The JSON response containing game achievements.
    """
    
    game_achievements_url = f'http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={appid}&key={api_key}&steamid={steam_user_id}'
    response = make_request(game_achievements_url, retry=4, wait=2)

    if response is not None and response.status_code == 200:
        json_game_achievements = response.json()
        return json_game_achievements
    else:
        print(f'Error fetching achievements for appid {appid}. Please check the appid or your API key.')
        return None


for indesx, acvt in tqdm(df_game_information_final.iterrows(), desc="Fetching achievements", total=len(df_game_information_final), ncols=100):
    selected_appid = acvt['steam_game_id']

    game_achievements_url = f'http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={selected_appid}&key={API_KEY}&steamid={steam_user_id}'

    acheivements_json = get_game_achievements(steam_user_id, API_KEY, selected_appid )

    if acheivements_json is None:
        no_information_games.append(selected_appid)
        continue
    else:
        acheivements_info = acheivements_json.get('playerstats')
    
    acheivements_info['appid'] = selected_appid
    
    collect_achievements_info.append(acheivements_info)


df_achievements  = pd.DataFrame(collect_achievements_info)
print(f'total games whith no achievements: {len(no_information_games)}')


df_achievements_raw = pd.DataFrame(
    {
        'steam_user_id': df_achievements['steamID'],
        'steam_game_id': df_achievements['appid'],
        'appid': df_achievements['gameName'],
        'total_game_acheivements': df_achievements['achievements'].apply(lambda x: len(x) if isinstance(x, list) else 0),
        'total_game_acheivements_unlocked': df_achievements['achievements'].apply(lambda x: sum(item['achieved'] for item in x) if isinstance(x, list) else 0)
    }
)

df_achievements_final = df_achievements_raw.drop_duplicates(subset=['steam_game_id'])

df_game_status = pd.merge(df_game_information_final, df_achievements_final, on='steam_game_id', how='inner')

df_join_w_game = df_game_status.drop(columns=['steam_user_id_y', 'has_community_visible_stats','appid'])
df_join_w_user = pd.merge(df_user_final, df_join_w_game, left_on='steamid', right_on='steam_user_id_x', how='inner')

df_final = df_join_w_user.drop(columns=['steam_user_id_x', 'communityvisibilitystate', 'profilestate', 'avatarmedium'])



player_name = df_final['personaname'] if df_final['steamid'].iloc[0] == steam_user_id else None

player_name.iloc[0]

file_name = f"{player_name.iloc[0]}_steam_data_{datetime.now().strftime('%Y%m%d')}"
file_name = file_name.replace(' ', '_')


option = int(input("Select an option:\n1. Save as CSV\n2. Save as Excel\n3. Save as JSON\n"))
cust_path = input("Do you want to save the file in a custom path? (yes/no): ").lower()

root = tk.Tk()

if cust_path == 'yes':
    file_path = filedialog.askdirectory(title="Select a directory")
    print(f"Selected directory: {file_path}")
else:
    print("Using current directory for saving files.")



if option == 1:
    if cust_path == 'yes':
        df_final.to_csv(f"{file_path}/{file_name}.csv", index=False)
    else:
        df_final.to_csv(f"{file_name}.csv", index=False)
elif option == 2:
    if cust_path == 'yes':
        df_final.to_excel(f"{file_path}/{file_name}.xlsx", index=False)
    else:
        df_final.to_excel(f"{file_name}.xlsx", index=False)
elif option == 3:
    if cust_path == 'yes':
        df_final.to_json(f"{file_path}/{file_name}.json", orient='records', lines=True)
    else:
        df_final.to_json(f"{file_name}.json", orient='records', lines=True)



