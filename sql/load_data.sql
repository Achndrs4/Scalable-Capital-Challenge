-- First, just create a table that can read in our file
CREATE TEMPORARY TABLE temp_json_table AS
    SELECT *
    FROM read_json('{json_file_path}',
                   format = 'newline_delimited',
                   columns = {
                       listened_at: 'BIGINT',
                       recording_msid: 'VARCHAR',
                       user_name: 'VARCHAR',
                       track_metadata: 'JSON',
                       track_metadata_additional_info: 'JSON',
                       artist_name: 'VARCHAR',
                       track_name: 'VARCHAR',
                       release_name: 'VARCHAR'
                   });

-- Flatten the table to extract nested fields
CREATE TEMPORARY TABLE flattened_json_table AS
    SELECT
        listened_at,
        recording_msid,
        user_name,
        -- Flattening the 'track_metadata' and its nested 'additional_info' object
        track_metadata->'additional_info'->>'release_msid' AS album_msid,
        track_metadata->'additional_info'->>'artist_msid' AS artist_msid,
        track_metadata->>'artist_name' AS artist_name,
        track_metadata->>'track_name' AS track_name,
        track_metadata->>'release_name' AS album_name
    FROM temp_json_table;

-- Create permanent tables to store normalized data
CREATE TABLE IF NOT EXISTS users AS
    SELECT DISTINCT user_name
    FROM flattened_json_table;

CREATE TABLE IF NOT EXISTS artists AS
    SELECT DISTINCT artist_name, artist_msid
    FROM flattened_json_table;

CREATE TABLE IF NOT EXISTS  albums AS
    SELECT DISTINCT album_name, album_msid
    FROM flattened_json_table;

-- Tracks are linked to albums by album_msid
CREATE TABLE IF NOT EXISTS  tracks AS
    SELECT DISTINCT track_name, album_msid
    FROM flattened_json_table;

-- Create the listens table with foreign key references
CREATE TABLE IF NOT EXISTS listens AS
    SELECT
        recording_msid,
        listened_at,
        user_name,   -- Connects to the `users` table
        album_msid,  -- Connects to the `albums` table
        artist_msid, -- Connects to the `artists` table
        track_name   -- Connects to the `tracks` table
    FROM flattened_json_table;

---- Add foreign key constraints to ensure data integrity
---- These constraints are optional and can be added as needed
--ALTER TABLE listens
--
--ALTER TABLE listens
--ADD FOREIGN KEY (album_msid) REFERENCES albums(album_msid);
--
--ALTER TABLE listens
--ADD FOREIGN KEY (artist_msid) REFERENCES artists(artist_msid);
--
--ALTER TABLE listens
--ADD FOREIGN KEY (track_name, album_msid) REFERENCES tracks(track_name, album_msid);
