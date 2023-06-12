from collections import Counter
import mysql.connector
import networkx as nx
from faker import Faker
import requests
import random
import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine
import numpy as np

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

# Insert 10 songs for missing values
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


cnx.commit()
engine = create_engine('mysql+mysqlconnector://root:Pass123.@localhost/common_db')
df = pd.read_sql_query("SELECT * FROM songs", engine)
df1 = pd.read_sql_query("SELECT * FROM bands", engine)
df2 = pd.read_sql_query("SELECT * FROM users", engine)



def detect_duplicates(df, table_name):

    # Checks the table name to return the right df and creates the lists of duplicates
    if table_name == "songs":

        duplicates = df[df.duplicated(subset='title')]

        if duplicates.empty:
            print("No duplicates found.")
        else:
            num_duplicates = len(duplicates)
            print(f"{num_duplicates} duplicates found.")

        ids = duplicates["id"]
        title = duplicates["title"]
        band_id = duplicates["band_id"]
        user_id = duplicates["user_id"]

        print("Duplicate Song IDs: ", list(ids))
        print("Duplicate Song Titles: ", list(title))
        print("Duplicate Band IDs: ", list(band_id))
        print("Duplicate User IDs: ", list(user_id))

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
            num_duplicates = len(duplicates)
            print(f"{num_duplicates} duplicates found.")

            #extract the info from the duplicate bands
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


        ids = duplicates["id"]
        name = duplicates["name"]
        genre = duplicates["genre"]
        print("Duplicate Band IDs: ", list(ids) )
        print("Duplicate Band Names: ", list(name))
        print("Duplicate Band Genres: ", list(genre))

        select_query = ("SELECT * FROM bands")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'name', 'genre'])
        return df

    elif table_name == "users":

        duplicates = df[df.duplicated(subset='username', keep=False)]

        if duplicates.empty:
            print("No duplicates found.")
        else:
            num_duplicates = len(duplicates)
            print(f"{num_duplicates} duplicates found.")

            # Group the duplicates by username and update the second user's information
            groups = duplicates.groupby('username')
            for _, group in groups:
                group_size = len(group)
                if group_size > 1:
                    # Keep the first user in the group
                    keep_user = group.iloc[0]
                    keep_user_id = keep_user['id']

                    # Update the second user's information
                    for index, row in group.iterrows():
                        if row['id'] != keep_user_id:
                            # Generate new information for the second user
                            new_username = fake.user_name()
                            new_email = fake.email()

                            # Update the second user's information in the DataFrame
                            df.loc[index, 'username'] = new_username
                            df.loc[index, 'email'] = new_email

                            # Update the second user's information in the database table
                            update_query = f"UPDATE users SET username = '{new_username}', email = '{new_email}' WHERE id = {row['id']}"
                            cursor.execute(update_query)
                        cnx.commit()


        select_query = ("SELECT * FROM users")
        cursor.execute(select_query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'username', 'email', 'password', 'age', 'songs', 'bands'])
        return df

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

def detect_outliers(df):

    age_outliers_ids = []
    num_cols = ['age', 'songs', 'bands']
    for col in num_cols:
        data = df[col].values
        data_std = np.std(data)
        data_mean = np.mean(data)
        anomaly_cut_off = data_std * 3
        lower_limit = data_mean - anomaly_cut_off
        upper_limit = data_mean + anomaly_cut_off

        outliers = df[(data > upper_limit) | (data < lower_limit)]
        if col == 'age':
            age_outliers_ids = outliers['id'].tolist()

        if outliers.empty:
           print(f"No outliers found in column {col}.")
        else:
            num_outliers = len(outliers)
            print(f"{num_outliers} outliers found in column {col}:")
            print(outliers)

    return age_outliers_ids


print("\n *** SONGS TABLE ***")
df = detect_missing_values(df, "songs")
df = detect_duplicates(df, "songs")

print("\n *** BANDS TABLE ***")
df1 = detect_missing_values(df1, "bands")
df1 = detect_duplicates(df1, "bands")

print("\n *** USERS TABLE ***")
df2 = detect_missing_values(df2, "users")
df2 = detect_duplicates(df2, "users")

# Count the number of bands associated with each user
cursor.execute("""
UPDATE users
SET bands = (
    SELECT COUNT(DISTINCT band_id)
    FROM songs
    WHERE songs.user_id = users.id
)
WHERE id > 2
""")
# Count the number of songs associated with each user, excluding the first two users
cursor.execute("""
UPDATE users
SET songs = (
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT title, user_id
        FROM songs
    ) AS unique_songs
    WHERE unique_songs.user_id = users.id
)
WHERE id > 2
""")

# Fetch data from the users table
select_query = "SELECT * FROM users"
cursor.execute(select_query)
users_data = cursor.fetchall()

users_df = pd.DataFrame(users_data, columns=['id', 'username', 'email', 'password', 'age', 'songs', 'bands'])
age_outliers_ids = detect_outliers(users_df)

print("\nThe id of the user with age outlier is :  ", age_outliers_ids)
# Filter users DataFrame to exclude outliers in the 'age' column
filtered_users_df = users_df[~users_df['id'].isin(age_outliers_ids)]

average_age = str(filtered_users_df["age"].mean())
print("\nThe average age of the users is ", average_age)

# Define the age ranges and find the percentage of it's one in the users table

age_ranges = [
    {"label": "18-24", "min_age": 18, "max_age": 24},
    {"label": "25-34", "min_age": 25, "max_age": 34},
    {"label": "35-44", "min_age": 35, "max_age": 44},
    {"label": "45-54", "min_age": 45, "max_age": 54},
    {"label": "55+", "min_age": 55, "max_age": float('inf')}
]

# Calculate the number of users in each age range
user_counts = []
for age_range in age_ranges:
    min_age = age_range["min_age"]
    max_age = age_range["max_age"]
    if max_age == float('inf'):
        count = len(filtered_users_df[filtered_users_df['age'] >= min_age])
    else:
        count = len(filtered_users_df[(filtered_users_df['age'] >= min_age) & (filtered_users_df['age'] <= max_age)])
    user_counts.append(count)

# Calculate the total number of users
total_users = sum(user_counts)

# Calculate the percentage of users in each age range
percentages = [(count / total_users) * 100 for count in user_counts]

# Visualize the percentage of users in each age range in a pie
labels = [age_range["label"] for age_range in age_ranges]
plt.pie(percentages, labels=labels, autopct='%1.1f%%')
plt.title("User Age Distribution")
plt.axis('equal')
plt.show()


select_query = "SELECT title FROM songs"
cursor.execute(select_query)

# Fetch all the data from the query result
data = cursor.fetchall()

# Create a DataFrame from the fetched data with column name 'title'
tracks = pd.DataFrame(data, columns=['title'])

song_counts = tracks['title'].value_counts()

# Finds the song with the most common title
most_common_song = song_counts.idxmax()
# Finds the song with the least common title
least_common_song = song_counts.idxmin()

# Finds the counts of the song with the most common title
most_common_count = song_counts[most_common_song]
# Finds the counts of the song with the least common title
least_common_count = song_counts[least_common_song]

print("\nSong with the most appearances:", most_common_song)
print("Count:", most_common_count)
print("\nSong with the least appearances:", least_common_song)
print("Count:", least_common_count)

# Fetch data from the users, bands, and songs tables
cursor.execute("SELECT * FROM users")
users_data = cursor.fetchall()

cursor.execute("SELECT * FROM bands")
bands_data = cursor.fetchall()

cursor.execute("SELECT * FROM songs")
songs_data = cursor.fetchall()

# Create DataFrames from the fetched data
users_df = pd.DataFrame(users_data, columns=['id', 'username', 'email', 'password', 'age', 'songs', 'bands'])
bands_df = pd.DataFrame(bands_data, columns=['id', 'name', 'genre'])
songs_df = pd.DataFrame(songs_data, columns=['id', 'title', 'band_id', 'user_id'])

# Join tables to get the band information for each song
songs_with_bands_df = songs_df.merge(bands_df, left_on='band_id', right_on='id', how='inner')

band_counts = songs_with_bands_df['name'].value_counts()

# Finds the band with the most common name
most_common_band = band_counts.idxmax()
# Finds the counts of the band with the most common name
most_common_band_count = band_counts[most_common_band]

# Finds the band with the least common name
least_common_band = band_counts.idxmin()
# Finds the counts of the band with the least common name
least_common_band_count = band_counts[least_common_band]

print("\nBand that appears most times:", most_common_band)
print("Count:", most_common_band_count)
print("\nBand that appears least times:", least_common_band)
print("Count:", least_common_band_count)

# Define the width of the bars
bar_width = 0.35

# Create a bar chart for the most common and least common songs
plt.bar([most_common_song, least_common_song], [most_common_count, least_common_count], width=bar_width)
plt.xlabel('Song')
plt.ylabel('Count')
plt.title('Most Common and Least Common Songs')

# Annotate the counts next to the song names
plt.text(most_common_song, most_common_count, str(most_common_count), ha='center', va='bottom')
plt.text(least_common_song, least_common_count, str(least_common_count), ha='center', va='bottom')
plt.show()

# Define the width of the bars
bar_width = 0.35

# Create a bar chart for the most common and least common bands
plt.bar([most_common_band, least_common_band], [most_common_band_count, least_common_band_count], width=bar_width)
plt.xlabel('Band')
plt.ylabel('Count')
plt.title('Most Common and Least Common Bands')

# Annotate the counts next to the band names
plt.text(most_common_band, most_common_band_count, str(most_common_band_count), ha='center', va='bottom')
plt.text(least_common_band, least_common_band_count, str(least_common_band_count), ha='center', va='bottom')
plt.show()

# Genres percentage for pie

# Join tables to get the genre information for each song
songs_with_genres_df = songs_df.merge(bands_df, left_on='band_id', right_on='id', how='inner')

# Calculate the number of listeners for each genre
genre_counts = songs_with_genres_df['genre'].value_counts()

# Top 3 genres
top_genres = genre_counts.head(3)

total_listeners = genre_counts.sum()

# Define the percentage of the genres
genre_percentages = (top_genres / total_listeners) * 100

labels = top_genres.index
sizes = genre_percentages.values
plt.pie(sizes, labels=labels,autopct='%1.1f%%')
plt.title('Top 3 Most Listened Genres')

# Display the pie chart
plt.show()

#Graph
community_graph = nx.barabasi_albert_graph(20, 3)

# Rename the node labels to range from 1 to 20
mapping = {node: node + 1 for node in community_graph.nodes()}
community_graph = nx.relabel_nodes(community_graph, mapping)

nx.draw(community_graph, with_labels=True)
plt.show()

# Retrieve neighbors for each user
for user in community_graph.nodes():
    neighbors = list(community_graph.neighbors(user))

    # Retrieve songs associated with the user
    cursor.execute("SELECT title FROM songs WHERE user_id = %s", (user,))
    user_songs = [song[0] for song in cursor.fetchall()]

    # Find the most frequent song among the neighbors for user 20
    if user == 1:
        print(f"\nUser {user} neighbors: {neighbors}")
        neighbor_songs = []
        for neighbor in neighbors:
            cursor.execute("SELECT title FROM songs WHERE user_id = %s", (neighbor,))
            neighbor_songs += [song[0] for song in cursor.fetchall()]

        # Retrieve songs associated with user 20
        cursor.execute("SELECT title FROM songs WHERE user_id = %s", (user,))
        user_songs = [song[0] for song in cursor.fetchall()]

        # Remove the songs that user 20 already has from the neighbor songs
        neighbor_songs = [song for song in neighbor_songs if song not in user_songs]

        counter = Counter(neighbor_songs)
        most_common_song = counter.most_common(1)
        if most_common_song:
            print(f"User 1 should listen to: {most_common_song[0][0]}")

cnx.commit()
cursor.close()
cnx.close()

