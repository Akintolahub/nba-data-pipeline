SELECT
    CAST(id AS INTEGER) AS team_id,
    CAST(full_name AS STRING) AS full_name,
    CAST(abbreviation AS STRING) AS abbreviation,
    cast(nickname AS STRING) AS nickname,
    CAST('{{ ds }}' AS DATETIME) AS processed_date_time
FROM `nfl1-447014.nba_landing.nba_teams`;