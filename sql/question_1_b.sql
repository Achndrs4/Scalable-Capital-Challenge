 SELECT COUNT(DISTINCT user_name) AS march_1_2019_listeners
    FROM listens
    WHERE CAST(TO_TIMESTAMP(listened_at) AS DATE) = '2019-03-01';