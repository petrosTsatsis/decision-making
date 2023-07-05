import mysql.connector
from faker import Faker
import requests
import random
import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine
import missingno as msno
from fuzzywuzzy import fuzz

cnx = mysql.connector.connect(user='root', password='Pass123.',
                              host='localhost', database='common_db')
cursor = cnx.cursor()

# Drop songs table
cursor.execute("DROP TABLE IF EXISTS songs")
# Drop users table
cursor.execute("DROP TABLE IF EXISTS users")
# Drop bands table
cursor.execute("DROP TABLE IF EXISTS bands")

# Create users table
cursor.execute("""
CREATE TABLE users ( 
  id INT NOT NULL AUTO_INCREMENT,
  username VARCHAR(255),
  email VARCHAR(255),
  password VARCHAR(255),
  age INT, songs INT,
  bands INT, 
  PRIMARY KEY (id)
); """)

# Create bands table
cursor.execute("""
CREATE TABLE bands (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(255),
  genre VARCHAR(255),
  PRIMARY KEY (id)
);
""")

# Create songs table
cursor.execute("""
CREATE TABLE songs (
  id INT NOT NULL AUTO_INCREMENT,
  title VARCHAR(255),
  band_id INT,
  user_id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (band_id) REFERENCES bands(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
""")

# Insert 2 users manually for the duplicate problem
users = [("JaneDoe", None, None, "300", "5", "19"),
         ("JaneDoe", "smith@example.com", "12345", None, "90", "6")]

fake = Faker()

# Use faker to set the users data
for i in range(18):
    username = fake.user_name()
    email = fake.email()
    password = fake.password()
    age = random.randint(18, 65)
    songs = 0
    bands = 0
    users.append((username, email, password, age, songs, bands))

insert_query = "INSERT INTO users (username, email, password, age, songs, bands) VALUES (%s, %s, %s, %s, %s, %s)"
cursor.executemany(insert_query, users)

# Insert 10 bands for missing values
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, NULL)")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'rock')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'hip-hop')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'rap')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'hip-hop')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'rap')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'hip-hop')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'rap')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'hip-hop')")
cursor.execute("INSERT INTO bands (name, genre) VALUES (NULL, 'rap')")

# Insert 5 songs for missing values
cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 1, NULL)")
cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 2, 1)")
cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, NULL, 1)")
cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 1, 1)")
cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, NULL, 1)")

select_query = "SELECT * FROM users"
cursor.execute(select_query)
users = cursor.fetchall()
all_tracks = []
all_bands = []


def get_track_info(user_id, band_id, title):
    # Insert track into songs table
    insert_query = "INSERT INTO songs (title, band_id, user_id) VALUES (%s, %s, %s)"
    cursor.execute(insert_query, (title, band_id, user_id))


def get_band_info(band_name):

    # Check if band exists in bands table, if not, insert it and get genre and country
    select_query = "SELECT * FROM bands WHERE name=%s"
    cursor.execute(select_query, (band_name,))
    band = cursor.fetchone()

    if not band:
        # Get genre information for new band
        band_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={band_name}&api_key=8c28cd480b1cc42e83d4eff224e70ec6&format=json"
        band_response = requests.get(band_url)
        if band_response.ok:
            band_info = band_response.json()['artist']
            genre = band_info['tags']['tag'][0]['name'] if (band_info['tags'] and len(band_info['tags']['tag']) > 0) else 'Unknown'

        else:
            genre = 'Unknown'

        # Insert new band into bands table
        insert_query = "INSERT INTO bands (name, genre) VALUES (%s, %s)"
        cursor.execute(insert_query, (band_name, genre))
        cnx.commit()

        # Get inserted band id
        select_query = "SELECT id FROM bands WHERE name=%s"
        cursor.execute(select_query, (band_name,))
        band_id = cursor.fetchone()[0]
    else:
        band_id = band[0]

    return band_id

for user in users[1:]:

    # Insert missing values in user 8
    if user[0] == 8:
        cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 3, 8)")
        cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 3, 8)")
        cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 4, NULL)")
        cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, NULL, 8)")
        cursor.execute("INSERT INTO songs (title, band_id, user_id) VALUES (NULL, 5, 8)")
        continue

    # Retrieve 5 tracks for a random jenre
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettoptracks&tag={random.choice(['rock', 'pop', 'jazz', 'metal', 'hip hop', 'country', 'classical'])}&api_key=8c28cd480b1cc42e83d4eff224e70ec6&format=json&limit=5&album=1"
    response = requests.get(url)
    if response.ok:
        tracks = response.json()['tracks']['track']

        for track in tracks:

            title = track['name']
            all_tracks.append(title)
            band_name = track['artist']['name']
            all_bands.append(band_name)

            # Get band info and insert into bands table if not exists
            band_id = get_band_info(band_name)

            # Insert track info into songs table
            get_track_info(user[0], band_id, title)

engine = create_engine('mysql+mysqlconnector://root:Pass123.@localhost/common_db')
df = pd.read_sql_query("SELECT * FROM songs", engine)
df1 = pd.read_sql_query("SELECT * FROM bands", engine)
df2 = pd.read_sql_query("SELECT * FROM users", engine)

msno.bar(df)
plt.title("Songs Bar")
plt.show()
msno.heatmap(df)
plt.title("Songs Heatmap")
plt.show()
msno.matrix(df)
plt.title("Songs Matrix")
plt.show()
msno.dendrogram(df)
plt.title("Songs Dendrogram")
plt.show()

msno.bar(df1)
plt.title("Bands Bar")
plt.show()
msno.heatmap(df1)
plt.title("Bands Heatmap")
plt.show()
msno.matrix(df1, color = (0.27, 0.52, 1.0))
plt.title("Bands Matrix")
plt.show()
msno.dendrogram(df1, method = "median")
plt.title("Bands Dendrogram")
plt.show()

msno.bar(df2, sort = "ascending")
plt.title("Users Bar")
plt.show()
msno.heatmap(df2, cmap = "RdYlGn")
plt.title("Users Heatmap")
plt.show()
msno.matrix(df2)
plt.title("Users Matrix")
plt.show()
msno.dendrogram(df2, orientation = "right")
plt.title("Users Dendrogram")
plt.show()
def detect_missing_values(df,table_name):

    missing_values = df.isnull().sum()

    if missing_values.sum() == 0:
        print("No missing values found.")
    else:
        print("Missing values found:")
        print(missing_values)

        # Retrieve missing songs
        select_query = "SELECT * FROM songs WHERE title IS NULL"
        cursor.execute(select_query)
        missing_songs = cursor.fetchall()

        # Retrieve all songs for each user and keep the association between user_id and song
        all_user_songs = {}
        select_query = "SELECT user_id, title FROM songs"
        cursor.execute(select_query)
        for user_id, song_title in cursor.fetchall():
            if user_id not in all_user_songs:
                all_user_songs[user_id] = set()
            all_user_songs[user_id].add(song_title)

        # Fill in missing song titles with random values
        for record in missing_songs:
            user_id = record[3]
            track_name = random.choice(all_tracks)

            while track_name in all_user_songs.get(user_id, set()):
                track_name = random.choice(all_tracks)

            update_query = "UPDATE songs SET title = %s WHERE id = %s"
            cursor.execute(update_query, (track_name, record[0]))

        select_query = "SELECT * FROM bands WHERE name IS NULL"
        cursor.execute(select_query)
        missing_bands_names = cursor.fetchall()

        # Fill in missing band titles with random values
        for record in missing_bands_names:
            band_name = random.choice(all_bands)
            update_query = "UPDATE bands SET name = %s WHERE id = %s"
            cursor.execute(update_query, (band_name, record[0]))

        # Check for missing band IDs in songs table
        select_query = "SELECT * FROM songs WHERE band_id IS NULL"
        cursor.execute(select_query)
        missing_bands_id = cursor.fetchall()

        # Fill in missing band IDs
        for record in missing_bands_id:
            band_name = record[1]  # Get band name from songs table
            band_id = get_band_info(band_name)  # Get band id from bands table
            update_query = "UPDATE songs SET band_id = %s WHERE id = %s"
            cursor.execute(update_query, (band_id, record[0]))

        # Check for missing band genres in bands table
        select_query = "SELECT * FROM bands WHERE genre IS NULL"
        cursor.execute(select_query)
        missing_bands_genres = cursor.fetchall()

        # Fill in missing band genres
        for record in missing_bands_genres:
            band_genre = random.choice(['rock', 'pop', 'jazz', 'metal', 'hip hop', 'country', 'classical'])
            update_query = "UPDATE bands SET genre = %s WHERE id =%s"
            cursor.execute(update_query, (band_genre, record[0]))

        # Check for missing user IDs in songs table
        select_query = "SELECT * FROM songs WHERE user_id IS NULL"
        cursor.execute(select_query)
        missing_users_id = cursor.fetchall()

        # Fill in missing user IDs
        for record in missing_users_id:
            user_id = (record[0] - 1) // 5 + 1  # Calculate user ID based on song ID
            update_query = "UPDATE songs SET user_id = %s WHERE id = %s"
            cursor.execute(update_query, (user_id, record[0]))

        # Check for missing usernames in users table
        select_query = "SELECT * FROM users WHERE username IS NULL"
        cursor.execute(select_query)
        missing_username = cursor.fetchall()

        # Fill in missing username
        for user in missing_username:
            username = fake.user_name()
            update_query = "UPDATE users SET username = %s WHERE id = %s"
            cursor.execute(update_query, (username, user[0] ))

        # Check for missing emails in users table
        select_query = "SELECT * FROM users WHERE email IS NULL"
        cursor.execute(select_query)
        missing_email = cursor.fetchall()

        # Fill in missing email
        for user in missing_email:
            email = fake.email()
            update_query = "UPDATE users SET email = %s WHERE id = %s"
            cursor.execute(update_query, (email, user[0] ))

        # Check for missing passwords in users table
        select_query = "SELECT * FROM users WHERE password IS NULL"
        cursor.execute(select_query)
        missing_password = cursor.fetchall()

        # Fill in missing passwords
        for user in missing_password:
            password = fake.password()
            update_query = "UPDATE users SET password = %s WHERE id = %s"
            cursor.execute(update_query, (password, user[0] ))

        # Retrieve users with missing ages
        select_query = "SELECT * FROM users WHERE age IS NULL"
        cursor.execute(select_query)
        missing_age_users = cursor.fetchall()

        # Fill in missing ages
        for user in missing_age_users:
            age = random.randint(18, 65)
            update_query = "UPDATE users SET age = %s WHERE id = %s"
            cursor.execute(update_query, (age, user[0]))

     # Checks the table name to return the right df
    if table_name == "songs":
        select_query = ("SELECT * FROM songs")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'title', 'band_id', 'user_id'])
        return df
    elif table_name == "bands":
        select_query = ("SELECT * FROM bands")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'name', 'genre'])
        return df
    elif table_name == "users":
        select_query = ("SELECT * FROM users")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'username', 'email', 'password', 'age', 'songs', 'bands'])
        return df

def detect_duplicates(df, table_name):

    # Checks the table name to return the right df and creates the lists of duplicates
    if table_name == "songs":

        duplicate_titles = set()
        found_duplicates = set()

        for i in range(len(df)):
            song = df.iloc[i]['title']
            if song in found_duplicates:
                continue
            for j in range(i + 1, len(df)):
                other_song = df.iloc[j]['title']

                if fuzz.ratio(song, other_song) == 100:
                    duplicate_titles.add(song)
                    found_duplicates.add(song)

        num_duplicates = len(duplicate_titles)
        print(f"\nDuplicate Songs: {num_duplicates} found.")
        for title in duplicate_titles:
            print(f"- {title}")

        select_query = ("SELECT * FROM songs")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'title', 'band_id', 'user_id'])

        return df

    elif table_name == "bands":

        duplicates = df[df.duplicated(subset='name')]

        if duplicates.empty:
            print("No duplicates found.")
        else:
            duplicate_names = set()
            found_duplicates = set()
            duplicate_band_id = set()
            duplicate_band_genre = set()

            for i in range(len(df)):
                band = df.iloc[i]['name']
                if band in found_duplicates:
                    continue
                for j in range(i + 1, len(df)):
                    other_band = df.iloc[j]['name']
                    if fuzz.ratio(band, other_band) == 100:
                        duplicate_names.add(band)
                        duplicate_band_genre.add(df.iloc[j]['genre'])
                        duplicate_band_id.add(df.iloc[j]['id'])
                        found_duplicates.add(band)
                        found_duplicates.add(other_band)

            num_duplicates = len(duplicate_names)
            print(f"\nDuplicate Bands: {num_duplicates} found.")
            for name in duplicate_names:
                print(f"- {name}")

            print("\nDuplicate Band IDs:")
            for band_id in duplicate_band_id:
                print(f"- {band_id}")

            print("\nDuplicate Band Genres:")
            for genre in duplicate_band_genre:
                print(f"- {genre}")

            # Take the info from the duplicate bands
            for index, row in duplicates.iterrows():
                duplicate_band_id = row['id']
                existing_band = df[df['name'] == row['name']].iloc[0]
                existing_band_id = existing_band['id']

                # Update the foreign key references to point to the existing band ID
                update_query = f"UPDATE songs SET band_id = {existing_band_id} WHERE band_id = {duplicate_band_id}"
                cursor.execute(update_query)
                cnx.commit()

                # Remove the duplicate band from the database table
                delete_query = f"DELETE FROM bands WHERE id = {duplicate_band_id}"
                cursor.execute(delete_query)
                cnx.commit()

        select_query = ("SELECT * FROM bands")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'name', 'genre'])
        return df

    elif table_name == "users":

        duplicate_usernames = set()
        duplicate_emails = set()
        duplicate_user_ids = set()
        found_duplicates = set()

        for i in range(len(df)):

            username = df.iloc[i]['username']
            email = df.iloc[i]['email']

            if username in found_duplicates:
                continue
            if email in found_duplicates:
                continue

            for j in range(i + 1, len(df)):
                other_username = df.iloc[j]['username']
                other_email = df.iloc[j]['email']

                if fuzz.ratio(username, other_username) == 100:
                    duplicate_user_ids.add(df.iloc[j]['id'])
                    duplicate_user_ids.add(df.iloc[i]['id'])
                    duplicate_usernames.add(username)
                    found_duplicates.add(username)

                    # Generate new username for the second user
                    new_username = fake.user_name()

                    # Update the second user's username in the DataFrame
                    df.loc[j, 'username'] = new_username

                    # Update the second user's username in the database table
                    update_query = f"UPDATE users SET username = '{new_username}' WHERE id = {df.iloc[j]['id']}"
                    cursor.execute(update_query)

                if fuzz.ratio(email, other_email) == 100:

                    duplicate_user_ids.add(df.iloc[j]['id'])
                    duplicate_user_ids.add(df.iloc[i]['id'])
                    duplicate_emails.add(email)
                    found_duplicates.add(email)

                    # Generate new email for the second user
                    new_email = fake.email()

                    # Update the second user's email in the DataFrame
                    df.loc[j, 'email'] = new_email

                    # Update the second user's email in the database table
                    update_query = f"UPDATE users SET email = '{new_email}' WHERE id = {df.iloc[j]['id']}"
                    cursor.execute(update_query)

                cnx.commit()

        num_duplicate_usernames = len(duplicate_usernames)
        num_duplicate_emails = len(duplicate_emails)

        print(f"\nDuplicate Usernames: {num_duplicate_usernames} found.")
        for username in duplicate_usernames:
            print(f"- {username}")

        print(f"\nDuplicate Emails: {num_duplicate_emails} found.")
        for email in duplicate_emails:
            print(f"- {email}")

        print("\nDuplicate User IDs:")
        for user_id in duplicate_user_ids:
            print(f"- {user_id}")

        select_query = ("SELECT * FROM users")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'username', 'email', 'password', 'age', 'songs', 'bands'])
        return df

print("\n *** SONGS TABLE ***")
df = detect_missing_values(df, "songs")
df = detect_duplicates(df, "songs")

print("\n *** BANDS TABLE ***")
df1 = detect_missing_values(df1, "bands")
df1 = detect_duplicates(df1, "bands")

print("\n *** USERS TABLE ***")
df2 = detect_missing_values(df2, "users")
df2 = detect_duplicates(df2, "users")

cnx.commit()
cursor.close()
cnx.close()
