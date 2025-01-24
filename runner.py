import duckdb
import pandas as pd

# Step 1: Connect to DuckDB database (file-based)
con = duckdb.connect()

# Path to JSON file
json_file_path = "data/dataset.json"

# Step 2: Create a table from JSON with a defined schema
# We define the schema explicitly to ensure that all fields are correctly interpreted
con.execute("SET threads TO 4;")  # Set DuckDB to use multiple threads for faster processing

con.execute(f"""
    CREATE TEMPORARY TABLE temp_json_table AS
    SELECT *
    FROM read_json('{json_file_path}', 
                   format = 'newline_delimited', 
                   columns = {{
                       listened_at: 'BIGINT',
                       recording_msid: 'VARCHAR',
                       user_name: 'VARCHAR',
                       track_metadata: 'JSON',
                       track_metadata_additional_info: 'JSON',
                       artist_name: 'VARCHAR',
                       track_name: 'VARCHAR',
                       release_name: 'VARCHAR'
                   }})
""")

# Step 3: Flatten the nested JSON (track_metadata, and its subfields) to create a normalized table
con.execute("""
    CREATE TEMPORARY TABLE flattened_json_table AS
    SELECT
        listened_at,
        recording_msid,
        user_name,
        -- Flattening the 'track_metadata' and its nested 'additional_info' object
        track_metadata->'additional_info'->>'release_msid' AS album_msid,
        track_metadata->>'artist_name' AS artist_name,
        track_metadata->>'track_name' AS track_name,
        track_metadata->>'release_name' AS album_name
    FROM temp_json_table
""")

# Step 4: Create necessary tables (Listens, Artists, Tracks, Albums)
# Creating the Listens table
con.execute("""
    CREATE TABLE listens AS
    SELECT
        recording_msid,
        listened_at,
        user_name,
        album_msid,
        track_name
    FROM flattened_json_table
""")

# Creating the Artists table
con.execute("""
    CREATE TABLE artists AS
    SELECT DISTINCT artist_name, album_msid
    FROM flattened_json_table
""")

# Creating the Tracks table
con.execute("""
    CREATE TABLE tracks AS
    SELECT DISTINCT track_name, album_msid
    FROM flattened_json_table
""")

# Creating the Albums table
con.execute("""
    CREATE TABLE albums AS
    SELECT DISTINCT album_name, album_msid
    FROM flattened_json_table
""")

# Step 5: Query 1 - Top 10 users with the most listens
query1 = """
SELECT user_name, COUNT(*) AS number_of_listens
FROM listens
GROUP BY user_name
ORDER BY number_of_listens DESC
LIMIT 10;
"""

top_users = con.execute(query1).fetchdf()
print("Top 10 Users with Most Listens:")
print(top_users)

# Query 2: Number of Users Who Listened to Songs on 1st March 2019
query2 = """
    SELECT COUNT(DISTINCT user_name) AS users_listened_on_march_1_2019
    FROM listens
    WHERE CAST(TO_TIMESTAMP(listened_at) AS DATE) = '2019-03-01';
"""

users_on_march_1 = con.execute(query2).fetchdf()
print("\nUsers who listened to a song on 1st March 2019:")
print(users_on_march_1)

# Query 3: First Song Each User Listened to
query3 = """
    WITH ranked_listens AS (
        SELECT
            user_name,
            track_name,
            listened_at,
            ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY listened_at) AS rank
        FROM listens
    )
    SELECT user_name, track_name AS first_song
    FROM ranked_listens
    WHERE rank = 1;
"""
first_songs = con.execute(query3).fetchdf()
print("\nFirst song each user listened to:")
print(first_songs)

# Step 8: Query 4 - Top 3 days with the most listens per user
query4 = """
WITH daily_listens AS (
    SELECT user_name, CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listen_date, COUNT(*) AS number_of_listens
    FROM listens
    GROUP BY user_name, listen_date
)
SELECT user_name, listen_date, number_of_listens
FROM (
    SELECT user_name, listen_date, number_of_listens,
           RANK() OVER (PARTITION BY user_name ORDER BY number_of_listens DESC) AS rank
    FROM daily_listens
) AS ranked_listens
WHERE rank <= 3
ORDER BY user_name, number_of_listens DESC;
"""

top_3_days_per_user = con.execute(query4).fetchdf()
print("\nTop 3 days with the most listens per user:")
print(top_3_days_per_user)

# Step 9: Query 5 - Daily active users and percentage of active users
query5 = """
WITH active_users_per_day AS (
    SELECT
        user_name,
        CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listen_date
    FROM listens
    GROUP BY user_name, listen_date
),
active_users_within_window AS (
    SELECT
        a.user_name,
        a.listen_date,
        COUNT(DISTINCT a.listen_date) OVER (
            PARTITION BY a.user_name
            ORDER BY a.listen_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS active_in_window
    FROM active_users_per_day a
),
valid_users AS (
    -- Only keep users with at least 7 days of activity
    SELECT user_name
    FROM active_users_within_window
    GROUP BY user_name
    HAVING COUNT(DISTINCT listen_date) >= 7
),
daily_active_users AS (
    SELECT
        a.listen_date,
        COUNT(DISTINCT a.user_name) AS number_active_users
    FROM active_users_within_window a
    JOIN valid_users v ON a.user_name = v.user_name
    WHERE a.active_in_window > 0
    GROUP BY a.listen_date
),
total_users AS (
    SELECT COUNT(DISTINCT user_name) AS total_users_count
    FROM listens
)
SELECT
    d.listen_date AS date,
    d.number_active_users,
    (d.number_active_users * 100.0 / t.total_users_count) AS percentage_active_users
FROM daily_active_users d, total_users t
ORDER BY d.listen_date;
"""

daily_active_users = con.execute(query5).fetchdf()
print("\nDaily Active Users and Percentage Active Users:")
print(daily_active_users)
