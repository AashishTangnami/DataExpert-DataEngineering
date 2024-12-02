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

------ Dropping the table players ---

DROP TABLE players;


--- Creating an enum type called scoring_class -----
CREATE TYPE SCORING_CLASS AS ENUM('star', 'good', 'average', 'bad');

--- Creating a table for players with a data type struct for added scoring_class and year_since_last_season
CREATE TABLE PLAYERS (
	PLAYER_NAME TEXT,
	HEIGHT TEXT,
	COLLEGE TEXT,
	COUNTRY TEXT,
	DRAFT_YEAR TEXT,
	DRAFT_ROUND TEXT,
	DRAFT_NUMBER TEXT,
	SEASON_STATS SEASON_STATS[],
	SCORING_CLASS SCORING_CLASS,
	YEAR_SINCE_LAST_SEASON INTEGER,
	CURRENT_SEASON INTEGER,
	PRIMARY KEY (PLAYER_NAME, CURRENT_SEASON)
);


---------- Inserting data into the players table ------------

INSERT INTO
	PLAYERS
WITH
	YESTERDAY AS (
		SELECT
			*
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 1995
	),
	TODAY AS (
		SELECT
			*
		FROM
			PLAYER_SEASONS
		WHERE
			SEASON = 1996
	)
SELECT
	COALESCE(T.PLAYER_NAME, Y.PLAYER_NAME) AS PLAYER_NAME,
	COALESCE(T.HEIGHT, Y.HEIGHT) AS HEIGHT,
	COALESCE(T.COLLEGE, Y.COLLEGE) AS COLLEGE,
	COALESCE(T.COUNTRY, Y.COUNTRY) AS COUNTRY,
	COALESCE(T.DRAFT_YEAR, Y.DRAFT_YEAR) AS DRAFT_YEAR,
	COALESCE(T.DRAFT_ROUND, Y.DRAFT_ROUND) AS DRAFT_ROUND,
	COALESCE(T.DRAFT_NUMBER, Y.DRAFT_NUMBER) AS DRAFT_NUMBER,
	CASE
		WHEN Y.SEASON_STATS IS NULL THEN ARRAY[
			ROW (T.SEASON, T.GP, T.PTS, T.REB, T.AST)::SEASON_STATS
		]
		WHEN T.SEASON IS NOT NULL THEN Y.SEASON_STATS || ARRAY[
			ROW (T.SEASON, T.GP, T.PTS, T.REB, T.AST)::SEASON_STATS
		]
		ELSE Y.SEASON_STATS
	END AS SEASON_STATS,
	CASE
		WHEN T.SEASON IS NOT NULL THEN CASE
			WHEN T.PTS > 20 THEN 'star'
			WHEN T.PTS > 15 THEN 'good'
			WHEN T.PTS > 10 THEN 'average'
			ELSE 'bad'
		END::SCORING_CLASS
		ELSE Y.SCORING_CLASS
	END,
	CASE
		WHEN T.SEASON IS NOT NULL THEN 1
	END,
	CASE
		WHEN T.SEASON IS NOT NULL THEN 0
		ELSE COALESCE(Y.YEARS_SINCE_LAST_SEASON, 0) + 1
	END AS YEARS_SINCE_LAST_SEASON COALESCE(T.SEASON, Y.CURRENT_SEASON + 1) AS CURRENT_SEASON
FROM
	TODAY T
	FULL OUTER JOIN YESTERDAY Y ON T.PLAYER_NAME = Y.PLAYER_NAME