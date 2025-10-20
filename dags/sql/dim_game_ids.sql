SELECT
        ROW_NUMBER() OVER (ORDER BY game_id DESC) AS game_key,
        CAST(game_id AS INTEGER) AS game_id,
        CAST(team_id AS INTEGER) AS team_id,
        CAST(game_date AS DATE) AS game_date,
        CAST(matchup AS STRING) AS matchup
FROM `nfl1-447014.nba_staging.nba_game_ids`