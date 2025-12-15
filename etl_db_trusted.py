import pandas as pd
import sqlite3

#---------------------- TURSTED DB FORMATION PROCESS ----------------------#

conn_raw = sqlite3.connect('database/steam_data_raw.db')
conn_trusted = sqlite3.connect('database/steam_data_trusted.db')

cursor = conn_trusted.cursor()

        #---------- PROFILE USER TABLE ----------#

try:
    sql_user_table = """
        CREATE TABLE IF NOT EXISTS profile_data
        (
            steamid TEXT,
            communityvisibilitystate INT,
            profilestate INT,
            avatarhash TEXT,
            profileurl TEXT,
            timecreated DATETIME,
            lastlogoff DATETIME,
            loccountrycode TEXT,
            avatarmedium TEXT,
            dh_updated DATETIME,   
            PRIMARY KEY (steamid)
        )
    """

    cursor.execute(sql_user_table)
    print("Table 'profile_data' created successfully or already exists.")
except Exception as e:
    print(f"An error occurred while creating the table: {e}")

        #---------- GET DATA FROM RAW ----------#

df_user_raw = pd.read_sql_query("SELECT * FROM profile_data", conn_raw)

        #---------- UPSERT OPERATION ----------#

sql_upsert = """
    INSERT INTO profile_data (
        steamid,
        communityvisibilitystate,
        profilestate,
        avatarhash,
        profileurl,
        timecreated,
        lastlogoff,
        loccountrycode,
        avatarmedium,
        dh_updated
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(steamid) DO UPDATE SET
        dh_updated = excluded.dh_updated
"""

try:
    for row in df_user_raw.itertuples(index=False):
        cursor.execute(sql_upsert, (
            row.steamid,
            row.communityvisibilitystate,
            row.profilestate,
            row.avatarhash,
            row.profileurl,
            row.timecreated,
            row.lastlogoff,
            row.loccountrycode,
            row.avatarmedium,
            row.dh_updated
        ))
    print("Upsert operation completed successfully.")
except Exception as e:
    print(f"An error occurred during the upsert operation: {e}")

#---------------------------------------------------------------------------------------------










        #---------- COLLECTION GAME TABLE ----------#

try:
    sql_collection_game_data = """
        CREATE TABLE IF NOT EXISTS collection_game_data 
        (
            steam_user_id TEXT,
            steam_game_id TEXT,
            name TEXT,
            last_played_timestamp DATETIME,
            playtime_forever INT,
            img_game_cover_url TEXT,
            playtime_2weeks FLOAT,
            dh_updated DATETIME,
            total_game_acheivements INT,
            total_game_acheivements_unlocked INT,
            PRIMARY KEY (steam_user_id, steam_game_id)
        )
    """

    cursor.execute(sql_collection_game_data)
    print("Table 'collection_game_data' created successfully or already exists.")
except Exception as e:
    print(f"An error occurred while creating the table: {e}")

        #---------- GET DATA FROM RAW ----------#

df_game_collection_raw = pd.read_sql_query("SELECT * FROM collection_game_data", conn_raw)

#print(df_game_collection_raw.dtypes)

        #---------- UPSERT OPERATION ----------#

sql_upsert = """
    INSERT INTO collection_game_data (
        steam_user_id,
        steam_game_id,
        name,
        last_played_timestamp,
        playtime_forever,
        img_game_cover_url,
        playtime_2weeks,
        dh_updated,
        total_game_acheivements,
        total_game_acheivements_unlocked
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(steam_user_id, steam_game_id) DO UPDATE SET
        dh_updated = excluded.dh_updated
"""

try:
    for row in df_game_collection_raw.itertuples(index=False):
        cursor.execute(sql_upsert, (
            row.steam_user_id_x,
            row.steam_game_id,
            row.name,
            row.last_played_timestamp,
            row.playtime_forever,
            row.img_game_cover_url,
            row.playtime_2weeks,
            row.dh_updated,
            row.total_game_acheivements,
            row.total_game_acheivements_unlocked
        ))
    print("Upsert operation completed successfully.")
except Exception as e:
    print(f"An error occurred during the upsert operation: {e}")









        #---------- GAME METADATA TABLE ----------#

try:
    sql_game_metadata_table = """
        CREATE TABLE IF NOT EXISTS game_metadata
        (
            steam_game_id TEXT PRIMARY KEY,
            name TEXT,
            required_age INT,
            is_free BOOLEAN,
            dlc tEXT,
            about_the_game tEXT,
            short_description TEXT,
            supported_languages TEXT,
            header_image TEXT,
            website TEXT,
            developers TEXT,
            publishers TEXT,
            genres TEXT,
            categories TEXT,
            media TEXT,
            dh_updated DATETIME
        )
    """

    cursor.execute(sql_game_metadata_table)
    print("Table 'game_metadata' created successfully or already exists.")
except Exception as e:
    print(f"An error occurred while creating the table: {e}")

        #---------- GET DATA FROM RAW ----------#

df_game_metadata_raw = pd.read_sql_query("SELECT * FROM game_metadata", conn_raw)

        #---------- UPSERT OPERATION ----------#

sql_upsert = """
    INSERT INTO game_metadata (
        steam_game_id,
        name,
        required_age,
        is_free,
        dlc,
        about_the_game,
        short_description,
        supported_languages,
        header_image,
        website,
        developers,
        publishers,
        genres,
        categories,
        media,
        dh_updated
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(steam_game_id) DO UPDATE SET
        dh_updated = excluded.dh_updated
"""

for row in df_game_metadata_raw.itertuples(index=False):
    try:
        cursor.execute(sql_upsert, (
            row.steam_game_id,
            row.name,
            row.required_age,
            row.is_free,
            row.dlc,
            row.about_the_game,
            row.short_description,
            row.supported_languages,
            row.header_image,
            row.website,
            row.developers,
            row.publishers,
            row.genres,
            row.categories,
            row.media,
            row.dh_updated          
        ))
    except Exception as e:
        print(f"An error occurred during the upsert operation{e}")



conn_trusted.commit()

conn_raw.close()
conn_trusted.close()