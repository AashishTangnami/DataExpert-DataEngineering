 -- Lecture Lab: 1

SELECT * FROM player_seasons;


--- Creating a Struct
CREATE TYPE season_stats AS (
	season  INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAL,
	ast REAL
);


-- Creating a table for players with a data type struct for season_stats
CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	colelge TEXT,
	country TEXT,
	draft_year TEXT, 
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);

--- 
ALTER TABLE players
RENAME COLUMN colelge TO college;

SELECT min(season) from player_seasons;


--- Inserting data into the players table.
INSERT INTO players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
),
 today AS ( SELECT * FROM player_seasons
 WHERE season = 2001
 )
 SELECT 
	 COALESCE(t.player_name , y.player_name ) AS player_name,
	 COALESCE(t.height , y.height ) AS height,
	 COALESCE(t.college , y.college ) AS college,
	 COALESCE(t.country , y.country ) AS country,
	 COALESCE(t.draft_year , y.draft_year ) AS draft_year,
	 COALESCE(t.draft_round , y.draft_round ) AS draft_round,
	 COALESCE(t.draft_number, y.draft_number ) AS draft_number,
	 CASE WHEN y.season_stats IS NULL
	 	THEN ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		)::season_stats]
 	 WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		)::season_stats]
	 ELSE y.season_stats
 	 END AS season_stats,
	  COALESCE (t.season, y.current_season + 1) as current_season
 FROM 
 today t FULL OUTER JOIN yesterday y 
 ON t.player_name = y.player_name

-------------- Querying the players table ----------------------
 WITH
	UNNESTED AS (
		SELECT
			PLAYER_NAME,
			UNNEST(SEASON_STATS)::SEASON_STATS AS SEASON_STATS
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 1998
			AND PLAYER_NAME = 'Micheal Jordan'
	)
SELECT
	PLAYER_NAME,
	(SEASON_STATS::SEASON_STATS).*
FROM
	UNNESTED