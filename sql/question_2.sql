WITH daily_listens AS (
    -- Calculate the total number of listens per user on each day
    -- Group by user_name and listen_date to get the count of listens for each user on each specific date
    SELECT user_name, CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listen_date, COUNT(*) AS number_of_listens
    FROM listens
    GROUP BY user_name, listen_date
)
SELECT user_name, listen_date, number_of_listens
-- We can use a ranking function to find the top 3 listens
FROM (
    SELECT user_name, listen_date, number_of_listens,
           RANK() OVER (PARTITION BY user_name ORDER BY number_of_listens DESC) AS rank
    FROM daily_listens
) AS ranked_listens
WHERE rank <= 3
ORDER BY user_name, number_of_listens DESC;