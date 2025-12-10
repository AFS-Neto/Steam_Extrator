import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
#from fpdf import FPDF
#from PIL import Image
#from io import BytesIO

from utilities.defs import get_user_id_by_vanity, get_user_info, get_owned_games, get_game_achievements, save_file_opt, ai_achievement_breakdown, save_to_sqlite, extract_game_metadatas

load_dotenv()

#76561199490364483 neto
#76561197985622277 theo

#------------------------------------Generic info
API_KEY = os.getenv('API_KEY')
ai_api_key=os.getenv("GPT_API_KEY")
#------------------------------------user identification

steam_user_id = get_user_id_by_vanity(API_KEY)

if steam_user_id is None:
    print("Please provide a valid Steam user ID or vanity name.")
    exit()
else:
    print(f"Steam User ID identified: {steam_user_id}")

print("Start extracting information...")

#------------------------------------Get all user informations by steam id

players_info = get_user_info(steam_user_id, API_KEY).get('response').get('players')[0]

df_steam_user = pd.DataFrame([players_info])

df_user_final = pd.DataFrame(
    {
    'steamid': df_steam_user['steamid'],
    'communityvisibilitystate': df_steam_user['communityvisibilitystate'],
    'profilestate': df_steam_user['profilestate'],
    'avatarhash': df_steam_user['avatarhash'],
    'personaname': df_steam_user['personaname'] if 'personaname' in df_steam_user.columns else None,
    'profileurl': df_steam_user['profileurl'],
    'timecreated': df_steam_user['timecreated'].apply(lambda x: datetime.fromtimestamp(x)) if 'timecreated' in df_steam_user.columns else None,
    'lastlogoff': df_steam_user['lastlogoff'].apply(lambda x : datetime.fromtimestamp(x)) if 'lastlogoff' in df_steam_user.columns else None,
    'loccountrycode': df_steam_user['loccountrycode'] if 'loccountrycode' in df_steam_user.columns else None,
    'avatarmedium': df_steam_user['avatarmedium'],
    'dh_updated': datetime.now()
    }
)

if df_user_final['communityvisibilitystate'].iloc[0] != 3:
    print(f'{df_user_final['personaname']} profile is private, cannot fetch games. Please make it public to continue.')
    exit()

#------------------------------------Get list of owned games

games_info = get_owned_games(steam_user_id, API_KEY).get('response').get('games')

if games_info is None:
    print(f'No games found for Steam ID {steam_user_id}. Please check the ID or if you profile is public.')
    exit()

df_game_information = pd.DataFrame(games_info)

df_game_information['steam_player_id'] = steam_user_id
df_game_information_filtered = df_game_information[df_game_information['playtime_forever'] > 0]

df_game_information_final = pd.DataFrame(
    {
        'steam_user_id': df_game_information_filtered['steam_player_id'],
        'steam_game_id': df_game_information_filtered['appid'],
        'name': df_game_information_filtered['name'],
        'last_played_timestamp': df_game_information_filtered['rtime_last_played'].apply(lambda x: datetime.fromtimestamp(x)) if 'rtime_last_played' in df_game_information_filtered.columns else None,
        'playtime_forever': df_game_information_filtered['playtime_forever'],
        'img_game_cover_url': df_game_information_filtered['img_icon_url'],
        'has_community_visible_stats': df_game_information_filtered['has_community_visible_stats'],
        'playtime_2weeks': df_game_information_filtered['playtime_2weeks'] if 'playtime_2weeks' in df_game_information_filtered.columns else None,
        'dh_updated': datetime.now()
    } 
)

#------------------------------------#Get list of game achievements #

collect_achievements_info = []
no_information_games = []

for indesx, game in tqdm(df_game_information_final.iterrows(), desc="Fetching achievements", total=len(df_game_information_final), ncols=100):
    selected_appid = game['steam_game_id']

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
#--------------------------------------------------- Joining dataframes

df_achievements_final = df_achievements_raw.drop_duplicates(subset=['steam_game_id'])

df_game_status = pd.merge(df_game_information_final, df_achievements_final, on='steam_game_id', how='inner')

df_final = df_game_status.drop(columns=['steam_user_id_y', 'has_community_visible_stats','appid'])

#------------------------------------#Extract game metadatas 

game_set = df_final['steam_game_id'].unique().tolist()

try:
    df_game_metadatas, num_not_found = extract_game_metadatas(game_set)
    print(f'Total games with no metadata found: {num_not_found}, {num_not_found/len(game_set)*100:.2f}%')
except Exception as e:
    print(f'Error extracting game metadatas: {e}')

#------------------------------------#options for user

while True:
    action = int(input(
        """
        select one of the available options:
            \n1. Extract file with informations\n2. Extract personal documentation\n3. Tips for achievements (AI)\n4. Save data to local database
        """))
    if action in [1, 2, 3, 4]:
        break
    else:
        print("Invalid option. Please select 1, 2, or 3.")

if action == 1:

#--------------------------------------------------- Building file name

    player_name = df_user_final['personaname'].iloc[0]
    file_name = f"{player_name}_steam_data_{datetime.now().strftime('%Y%m%d')}"
    file_name = file_name.replace(' ', '_')

    save_file_opt(df_final, file_name)
    print(f"Data saved successfully as {file_name}.")

#------------------------------------#Chat gpt to searching not unlock achievements #

if action == 3:
    ai_achievement_breakdown(ai_api_key, steam_user_id, API_KEY)
    
#------------------------------------#Save data to local sqlite database #

if action == 4:
    save_to_sqlite(df_final, table_name='collection_game_data', method='append')
    save_to_sqlite(df_user_final, table_name='profile_data', method='append')



