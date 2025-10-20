SELECT
    CAST(player_id AS INTEGER) AS player_id,
    CAST(first_name AS STRING) AS first_name,
    CAST(last_name AS STRING) AS last_name,
    CAST(full_name AS STRING) AS full_name,
    CAST(is_active AS BOOLEAN) AS is_active
FROM `nfl1-447014.nba_staging.nba_players`
