SELECT
    CAST(team_id AS INTEGER) AS team_id,
    CAST(full_name AS STRING) AS full_name,
    CAST(abbreviation AS STRING) AS abbreviation,
    cast(nickname AS STRING) AS nickname
FROM `nfl1-447014.nba_staging.nba_teams`;