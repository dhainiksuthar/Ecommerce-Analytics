SELECT
    traffic_source, count(event_id) AS noOfEvents
FROM
    {{ref("stg_clickstream")}}
GROUP BY traffic_source
