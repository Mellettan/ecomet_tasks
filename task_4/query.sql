SELECT
    phrase,
    arraySort(x -> -x.1, groupArray((hour, views_diff))) AS views_by_hour
FROM
(
    SELECT
        phrase,
        hour,
        (max_views - lag(max_views, 1, 0) OVER (PARTITION BY phrase ORDER BY hour ASC)) AS views_diff
    FROM
    (
        SELECT
            phrase,
            toHour(dt) AS hour,
            max(views) AS max_views
        FROM phrases_views
        WHERE
            campaign_id = 1111111
            AND toDate(dt) = '2025-01-01'
        GROUP BY
            phrase,
            hour
    )
)

WHERE views_diff > 0
GROUP BY phrase