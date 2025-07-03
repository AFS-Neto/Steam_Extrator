import pandas as pd
import requests
import time
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm

API_KEY = '16093C534E54329C07BC8D32192C655A'

# user identification

print("""
Welcome to the Steam Data Extractor!
This tool allows you to extract Steam user data and game achievements.
      
Please select how you want to identify the user: 
      
    """
      )

id_choice = int(input("Select an option:\n1. By Steam ID\n2. By Vanity URL\n"))

if id_choice == 1:
    
    steam_user_id = input('Please enter the Steam ID (ex 765611994902323423): ')
    print(f'You selected to identify by Steam ID: {steam_user_id}')

elif id_choice == 2:
    vanity_name = input('Please enter the mane present into your profile URL: ')

    vanity_url = f'https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/?key={API_KEY}&vanityurl={vanity_name}'

    while True:
        try:
            response = requests.get(vanity_url)

            if response.status_code == 200:
                print(f'Connection successful, status code: {response.status_code}')
                json_vanity_info = response.json()

                if json_vanity_info.get('response').get('success') == 1:
                    steam_user_id = json_vanity_info.get('response').get('steamid')
                else:
                    print(f'Error: Vanity URL not found or does not exist. Please check the vanity name: {vanity_name} or use your steam ID.')
                    break

                break
            elif response.status_code == 429:
                print('Rate limit exceeded. Waiting for 2 seconds before retrying...')
                time.sleep(2)
                continue
            else:
                print(f'Error fetching vanity URL: {response.status_code}')
                break
        except requests.exceptions.RequestException as e:
            print(f'Error connecting to API: {e}')


#Get all user informations by steam id

user_info_url = f'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={API_KEY}&steamids={steam_user_id}'

while True:
    try:
        response = requests.get(user_info_url)

        if response.status_code == 429:
            print('Rate limit exceeded getting user informations. Waiting for 10 seconds before retrying...')
            time.sleep(10)
            continue
        elif response.status_code == 200:
            print(f'Connection successful, status code: {response.status_code}')
            json_user_info = response.json()
            break
    except requests.exceptions.RequestException as e:
        print(f'Error connecting to API: {e}')
        break



players_info = json_user_info.get('response').get('players')[0]
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


#Get list of owned games

games_info_url = f'https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={API_KEY}&steamid={steam_user_id}&include_appinfo=true&include_played_free_games=true&format=json'


while True:
    try:
        response = requests.get(games_info_url)

        if response.status_code == 429:
            print('Rate limit exceeded getting game list. Waiting for 10 seconds before retrying...')
            time.sleep(10)
            continue
        elif response.status_code == 200:
            print(f'Connection successful, status code: {response.status_code}')
            json_games_info = response.json()
            break
    except requests.exceptions.RequestException as e:
        print(f'Error connecting to API: {e}')
        break


games_info = json_games_info.get('response').get('games')

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

#Get list of game achievements

collect_achievements_info = []

for indesx, acvt in tqdm(df_game_information_final.iterrows(), desc="Fetching achievements", total=len(df_game_information_final), ncols=100):
    selected_appid = acvt['steam_game_id']

    game_achievements_url = f'http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={selected_appid}&key={API_KEY}&steamid={steam_user_id}'

    no_information_games = []

    try:
        response = requests.get(game_achievements_url)

        if response.status_code == 400:
            print(f'No achievements found for appid {selected_appid}.')
            no_information_games.append(selected_appid)

        # elif response.status_code == 429:
        #     print('Rate limit exceeded while fetching achievements. Waiting for 10 seconds before retrying...')
        #     time.sleep(10)
        #     continue
        
        elif response.status_code == 403:
            print(f'Access forbidden for appid {selected_appid}. This may be due to the game not having achievements or the profile being private.')
            no_information_games.append(selected_appid)

        elif response.status_code == 200:
             json_game_achievements = response.json()

    except Exception as e:
        print(f'Error fetching achievements for appid {selected_appid}: {e}')

    
    acheivements_info = json_game_achievements.get('playerstats')
    acheivements_info['appid'] = selected_appid
    
    collect_achievements_info.append(acheivements_info)
    
    time.sleep(2)

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



