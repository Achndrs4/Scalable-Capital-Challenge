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