SELECT
        CAST(game_id AS INTEGER) AS game_id,
        CAST(team_id AS INTEGER) AS team_id,
        CAST(game_date AS DATE) AS game_date,
        CAST(matchup AS STRING) AS matchup,
        CAST('{{ ds }}' AS DATETIME) AS processed_date_time
FROM `nfl1-447014.nba_landing.nba_game_ids`