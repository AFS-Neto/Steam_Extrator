import pandas as pd
import sqlite3

# ----------------- Connect to the SQLite database -----------------

conn = sqlite3.connect('database\steam_data_raw.db')

cursor = conn.cursor()

# ----------------- Query definition -----------------

game_query = """
    SELECT * from collection_game_data
"""

user_query = """
    SELECT * from profile_data
"""

# ----------------- Query Execution -----------------

cursor.execute(game_query)
game_data = cursor.fetchall()

game_data_columns = [g_description[0] for g_description in cursor.description]

cursor.execute(user_query)
user_data = cursor.fetchall()

user_data_columns = [u_description[0] for u_description in cursor.description]

# ----------------- Datafram shapping -----------------

df_game_data = pd.DataFrame(game_data, columns=game_data_columns)
df_user_data = pd.DataFrame(user_data, columns=user_data_columns)

# ----------------- Data Cleaning -----------------

df_user_data = df_user_data.sort_values(by='dh_updated', ascending=True)

df_user_data = df_user_data.drop_duplicates(subset=['steamid'], keep='last')

last_game_insert = df_game_data.groupby('steam_user_id_x')['dh_updated'].max().reset_index()

df_game_information_filtered = pd.merge(df_game_data, last_game_insert, left_on=['steam_user_id_x', 'dh_updated'], right_on=['steam_user_id_x', 'dh_updated'], how='inner')

print(df_game_information_filtered.dtypes)

# ----------------- Close first connection -----------------

conn.close()

# ----------------- Clean data deployed do db -----------------

conn = sqlite3.connect('database\steam_data_trusted.db')

cursor = conn.cursor()

# -----------------  Write cleaned data to the trusted database -----------------

df_user_data.to_sql('profile_data', conn, if_exists='replace', index=False)
df_game_information_filtered.to_sql('collection_game_data', conn, if_exists='replace', index=False)




