WITH active_users_per_day AS (
 -- Extract user activity (user name and activity date) from the listens table
 -- Each row represents an instance of a user listening to content on a specific day
    SELECT
        user_name,
        CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listen_date
    FROM listens
    GROUP BY user_name, listen_date
),
active_users_within_window AS (
-- For each user, count the number of distinct active days within a 7-day window
-- The window is defined as the current day and the previous 6 days (7 days in total)
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
    -- Select users who have at least 7 distinct days of activity within the window
    -- These users will be considered 'active' based on the criteria provided
    SELECT user_name
    FROM active_users_within_window
    GROUP BY user_name
    HAVING COUNT(DISTINCT listen_date) >= 7
),
daily_active_users AS (
    -- Now, find users who are actually active
    SELECT
        a.listen_date,
        COUNT(DISTINCT a.user_name) AS number_active_users
    FROM active_users_within_window a
    JOIN valid_users v ON a.user_name = v.user_name
    WHERE a.active_in_window > 0
    GROUP BY a.listen_date
),
total_users AS (
    -- Count the total number of unique users in the listens table
    -- This will be used to calculate the percentage of active users
    SELECT COUNT(DISTINCT user_name) AS total_users_count
    FROM listens
)
SELECT
    -- Final SELECT: Calculate the daily active user count and percentage of active users
    d.listen_date AS date,
    d.number_active_users,
    (d.number_active_users * 100.0 / t.total_users_count) AS percentage_active_users
FROM daily_active_users d, total_users t
ORDER BY d.listen_date;