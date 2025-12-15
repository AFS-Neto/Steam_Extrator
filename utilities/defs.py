import requests
import os
import pandas as pd
import time
import json
import tkinter as tk
from tkinter import filedialog
import questionary
import sqlite3
from openai import OpenAI



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

#----------------------------------------------------------------------

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
        
#----------------------------------------------------------------------

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
    

#----------------------------------------------------------------------

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
    

#----------------------------------------------------------------------

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
    
    game_achievements_url = f'http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={appid}&key={api_key}&steamid={steam_user_id}&l=en'
    response = make_request(game_achievements_url, retry=4, wait=2)

    if response is not None and response.status_code == 200:
        json_game_achievements = response.json()
        return json_game_achievements
    else:
        print(f'Error fetching achievements for appid {appid}. Please check the appid or your API key.')
        return None
    
#----------------------------------------------------------------------

def save_file_opt(df_final, file_name):

    while True:
        option = int(input("Select an option:\n1. Save as CSV\n2. Save as Excel\n3. Save as JSON\n"))
        if option in [1, 2, 3]:
            break
        else:
            print("Invalid option. Please select 1, 2, or 3.")

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

#-----------------------------------------------------------------------

def ai_achievement_breakdown(ai_api_key, steam_user_id, API_KEY):
    """
    Function to interact with OpenAI API to get achievement breakdown.
    """

    games_info = get_owned_games(steam_user_id, API_KEY).get('response').get('games')

    game_list = []

    for game in games_info:
        game_list.append(
            {
                "name": game['name'],
                "steam_game_id": game['appid']
            }
        )

    game_df = pd.DataFrame(game_list)

    game_df.sort_values(by='name')

    options = game_df['name'].to_list()

    selected_option = questionary.select(
        "Select a game to view unlocked achievements breakdonw:",
        choices=options
    ).ask()

    appid = game_df[game_df['name'] == selected_option]['steam_game_id'].iloc[0]

    print(f"You selected: {selected_option}({appid})")

    json_game_breakdown = get_game_achievements(steam_user_id, API_KEY, appid)

    client = OpenAI(
        api_key = ai_api_key
    )

    prompt = f"""
    You are a famous and respected video game journalist who specializes in helping players achieve 100% completion, platinum trophies, or 1000G achievements.

    I will provide you with:

    - The name of a game
    - A JSON file containing all achievements in this game, indicating which ones the player has unlocked and which ones remain locked.

    unlocked achievements is set to true, and locked achievements is set to false.

    For each locked achievement, please provide:

    1. The achievement name.
    2. A brief summary of what the player needs to do to unlock it.
    3. Helpful tips to achieve it.

    If an achievement has no description in the JSON, search for official information on the internet (e.g., game wikis, official sources).  
    - If you find reliable info, include it as the summary and tips.  

    for each achievement, please provide the information in the following format:
    - Achievement Name: [Achievement Name]
    - Summary: [Brief summary of what the player needs to do to unlock it]
    - Tips: [Helpful tips to achieve it]

    Please provide your response clearly and concisely, as if you are preparing a guide to be shown inside a gaming app.

    Here is the game name and the JSON data:

    Game: {selected_option}

    Achievements JSON:  
    {json_game_breakdown.get('playerstats').get('achievements')}
    """

    completion = client.chat.completions.create(
        model=os.getenv("GPT_MODEL"),
        store=True,
        temperature=0.5,
        max_tokens=800,
        messages=[
            {"role": "user", "content": prompt }
        ]
    )

    content = completion.choices[0].message.content
    print(content)

#---------------------------------- Send data to database ----------------------------------#

def save_to_sqlite(df, table_name, method ):
    """
    Save DataFrame to SQLite database.
    
    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - method (str): The method to use when saving ('append' or 'replace').
    """

    conn = sqlite3.connect('database/steam_data_raw.db')

    try:
        print(f'Checking if table {table_name} exists...')
        df.head(0).to_sql(table_name, conn, if_exists='fail', index=False)  # Create table
        print(f'Table {table_name} created successfully.')
    except Exception as e:
        print(f'Table {table_name} already exists. Proceeding to insert data.')

    print(f'Saving data to table {table_name} using method {method}... {len(df)} records to be inserted.')

    try:
        print(f'Inserting data into table {table_name}...')
        df.to_sql(table_name, conn, if_exists=method, index=False)
    except Exception as e:
        print(f'Error inserting data into table {table_name}: {e}')

    conn.close()


def extract_game_metadatas(game_list):

    game_set = game_list

    game_metadata_list = []
    not_found_games_ids = []

    for id in game_set:
        
        response = make_request(f'https://store.steampowered.com/api/appdetails?appids={id}')

        json_response = response.json()

        if not json_response.get(f'{id}').get('success'):
            print(f'No metadata found for game ID: {id}')
            not_found_games_ids.append(id)
            continue
        else:
            stage = json_response.get(f'{id}').get('data')

        df_game_metadata = {
            'steam_game_id': stage.get('steam_appid'),
            'name': stage.get('name'),
            'required_age': stage.get('required_age'),
            'is_free': stage.get('is_free'),
            'dlc': json.dumps(stage.get('dlc')) if stage.get('dlc') else None,
            'about_the_game': stage.get('about_the_game'),
            'short_description': stage.get('short_description'),
            'supported_languages': json.dumps(stage.get('supported_languages')) if stage.get('supported_languages') else None,
            'header_image': stage.get('header_image'),
            'website': stage.get('website'),
            'developers': json.dumps(stage.get('developers')) if stage.get('developers') else None,
            'publishers': json.dumps(stage.get('publishers')) if stage.get('publishers') else None,
            'genres': json.dumps(stage.get('genres')) if stage.get('genres') else None,
            'categories': json.dumps(stage.get('categories')) if stage.get('categories') else None,
            'media': json.dumps(stage.get('movies')) if stage.get('movies') else None,
        }

        game_metadata_list.append(df_game_metadata)

    df_game_metadata_final = pd.DataFrame(game_metadata_list)
    num_not_found = len(not_found_games_ids)

    return df_game_metadata_final, num_not_found