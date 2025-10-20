SELECT
DISTINCT players.full_name as player_name,
teams.full_name as team_name,
games.game_date,
ftsy.FAN_DUEL_PTS,
ftsy.NBA_FANTASY_PTS,
ftsy.PLUS_MINUS,
ftsy.PTS,
ftsy.AST,
ftsy.REB,
ftsy.STL,
ftsy.BLK,
ftsy.TOV,
ftsy.FG_PCT as field_goal_pct,
ftsy.FT_PCT as free_throw_pct,
ftsy.FG3_PCT as three_point_pct

FROM `nfl1-447014.nba_marts.fact_nba_fantasy_stats` ftsy
LEFT JOIN `nfl1-447014.nba_marts.dim_nba_players` players
  ON players.player_id = ftsy.PLAYER_ID
LEFT JOIN   `nfl1-447014.nba_marts.dim_nba_teams` teams
  ON teams.team_id = ftsy.TEAM_ID
LEFT JOIN `nfl1-447014.nba_marts.dim_game_ids` games
  ON games.game_id = ftsy.GAME_ID
  
where games.game_id is not null
and players.player_id is not null
and teams.team_id is not null

order by games.game_date desc


