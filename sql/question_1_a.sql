SELECT user_name, COUNT(*) AS number_of_listens
FROM listens
GROUP BY user_name
ORDER BY number_of_listens DESC
LIMIT 10;