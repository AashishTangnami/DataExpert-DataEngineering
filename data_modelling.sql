 -- Lecture Lab: 1
SELECT
	*
FROM
	PLAYER_SEASONS;



CREATE TYPE SEASON_STATS AS (
	SEASON INTEGER,
	GP INTEGER,
	PTS REAL,
	REB REAL,
	AST REAL
);

CREATE TYPE SCORING_CLASS AS ENUM('star', 'good', 'average', 'bad')

CREATE TABLE PLAYERS (
	PLAYER_NAME TEXT,
	HEIGHT TEXT,
	COLELGE TEXT,
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



--- Inserting data into the players table.
INSERT INTO
	PLAYERS
WITH
	YESTERDAY AS (
		SELECT
			*
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 2002
	),
	TODAY AS (
		SELECT
			*
		FROM
			PLAYER_SEASONS
		WHERE
			SEASON = 2002
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
		WHEN T.SEASON IS NOT NULL THEN 0
		ELSE COALESCE(Y.YEAR_SINCE_LAST_SEASON, 0) + 1
	END AS YEAR_SINCE_LAST_SEASON,
	
	COALESCE(T.SEASON, Y.CURRENT_SEASON + 1) AS CURRENT_SEASON
FROM
	TODAY T
	FULL OUTER JOIN YESTERDAY Y ON T.PLAYER_NAME = Y.PLAYER_NAME





WITH
	UNNESTED AS (
		SELECT
			PLAYER_NAME,
			UNNEST(SEASON_STATS)::SEASON_STATS AS SEASON_STATS --- season component un nested
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


SELECT * FROM PLAYERS
WHERE CURRENT_SEASON = 2000
AND PLAYER_NAME = 'Michael Jordan';


SELECT
	PLAYER_NAME,
	SEASON_STATS[1] AS FIRST_SEASON,
	SEASON_STATS[CARDINALITY(SEASON_STATS)] AS LATEST_SEASON
FROM
	PLAYERS
WHERE
	CURRENT_SEASON = 2001;

--- ANALYTICS ---
SELECT
	PLAYER_NAME,
	(
		SEASON_STATS[CARDINALITY(SEASON_STATS)]::SEASON_STATS
	).PTS / CASE
		WHEN (SEASON_STATS[1]::SEASON_STATS).PTS = 0 THEN 1
		ELSE (SEASON_STATS[1]::SEASON_STATS).PTS
	END AS AVG_STATS
FROM
	PLAYERS
WHERE
	CURRENT_SEASON = 2001 AND SCORING_CLASS = 'star';