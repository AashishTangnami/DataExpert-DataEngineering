-- Idempotent Pipelines
-- Definition:
    -- Idempotent: Denoting any operation that yields the same result regardless of the number of times it is applied or the conditions under which it is applied.
-- Key Characteristics:
    -- Pipelines should produce consistent results regardless of:
        -- The day you run it.
        -- The number of times you run it.
        -- The hour you run it.

-- What Can Make a Pipeline Non-Idempotent?
    -- INSERT INTO without TRUNCATE
        -- Use MERGE or INSERT OVERWRITE instead of INSERT INTO to ensure consistency.
    -- Using start_date > Without a Corresponding end_date <
        -- Leads to inconsistent results if date ranges are incomplete or overlapping.
    -- Not Using a Full Set of Partition Sensors
        -- The pipeline may run when data is missing or only partially available.
    -- Not Using depends_on_past for Cumulative Pipelines
        -- This can cause pipelines to process incomplete data for cumulative calculations.
    -- Relying on the "Latest" Partition of a Poorly Modeled SCD Table
        -- Example: Issues with "latest" partition in daily dimensions—this approach is error-prone.


-- Slowly Changing Dimensions (SCDs)
-- Definition:
    -- Slowly Changing Dimension (SCD): A dimension that evolves over time. Examples include attributes like age, height, weight, salary.


-- Modeling Dimensions That Change Over Time:
    -- Singular Snapshots
        -- Be cautious; these are not idempotent.
    -- Daily Partitioned Snapshots
        -- Preferred for idempotence.

    -- SCD Types (0, 1, 2, 3, etc.)


-- SCD Types
    -- SCD Type 0 (No Change)
        -- Example: A person's birthdate.
        -- Idempotent: Values are constant and unchanging.
    -- SCD Type 1 (Overwrite)
        -- Example: OLTP systems where only the latest value is stored.
        -- Not Idempotent: Backfilling overwrites data, losing historical accuracy.
    -- SCD Type 2 (Historical Tracking)
        -- Records changes over time using start_date and end_date.
        -- Current values often have:
            -- end_date = NULL, or
            -- A future date.
        -- Idempotent: Maintains history and allows accurate time-based filtering.
    -- SCD Type 3 (Original + Current)
        -- Tracks only "original" and "current" values.
            -- Benefits: Only one row per dimension.
            -- Drawbacks: No historical context between original and current values.
        -- Not Idempotent: Backfilling cannot determine transitions accurately.
    
    -- Hybrids (SCD Types 4, 5, 6, 7, 8, 9)
        -- These are hybrid types combining features of the above.
        -- Not Idempotent:
        -- Rarely used in practice due to complexity and lack of idempotence.
    
    
    -- Summary of Idempotence in SCD Types:
    -- Idempotent:
        -- Type 0: Values are unchanging.
        -- Type 2: Tracks changes with precise start/end dates.
    -- Not Idempotent:
        -- Type 1: Overwrites history.
        -- Type 3: Lacks full historical tracking.
        -- Types 4–9: Complex hybrids, not idempotent.


-- SCD2 Loading

    -- Load the entire history in one query.
        --  Inefficient but nimble.
        --  1 query and you're done.
    
    -- Incrementally load the data after the previous SCD is generated
        -- Has the same "depends_on_past" constraint/issue on cumulative pipelines.
        -- Efficient but cumbersome.


--- 
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
	IS_ACTIVE BOOLEAN, -- New column
	PRIMARY KEY (PLAYER_NAME, CURRENT_SEASON)
);


------ INSERTING DATA INTO THE PLAYERS TABLE ------
INSERT INTO
	PLAYERS
WITH
	YESTERDAY AS (
		SELECT
			*
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 2001
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
	COALESCE(T.SEASON, Y.CURRENT_SEASON + 1) AS CURRENT_SEASON,
	T.SEASON IS NOT NULL AS IS_ACTIVE
FROM
	TODAY T
	FULL OUTER JOIN YESTERDAY Y ON T.PLAYER_NAME = Y.PLAYER_NAME

----------- Insert Into Table Players using gnereate_Series ------------
INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;


    
--- CREATING TYPE 2 TABLE ---
--- SLOWLY CHANGING DIMENSION TYPE 2 TABLE---
CREATE TABLE PLAYERS_SCD (
	PLAYER_NAME TEXT,
	SCORING_CLASS SCORING_CLASS,
	IS_ACTIVE BOOLEAN,
	CURRENT_SEASON INTEGER,
	START_SEASON INTEGER,  -- start_date
	END_SEASON INTEGER, -- end_date
	PRIMARY KEY (PLAYER_NAME, START_SEASON)
)



SELECT
	PLAYER_NAME,
	CURRENT_SEASON,
	SCORING_CLASS,
	IS_ACTIVE,
    -- LAG() function is a window function that "Looks Back" at the previous row.
        -- It is used to retrieve the value of the previous row in the same result set, without the need for a self-join or subquery.
    -- Thus it retrieves the scroing_class from the previous season.
	LAG(SCORING_CLASS, 1) OVER (

        -- PARTITION BY: It is used to divide the player_name into partitions. 
        -- (same 'names' are grouped together and ordered by current_season)
		PARTITION BY
			PLAYER_NAME
		ORDER BY
			CURRENT_SEASON
	) AS PREVIOUS_SCORING_CLASS,
    -- It retrieves the is_active from the previous season.
	LAG(IS_ACTIVE, 1) OVER (
        -- (same 'names' are grouped together and ordered by current_season)
		PARTITION BY
			PLAYER_NAME
		ORDER BY
			CURRENT_SEASON
	) AS PREVIOUS_IS_ACTIVE
FROM
	PLAYERS



--------------- SCD2 TABLE ----------------
WITH
	WITH_PREVIOUS AS (
		SELECT
			PLAYER_NAME,
			CURRENT_SEASON,
			SCORING_CLASS,
			IS_ACTIVE,
			-- LAG() function is a window function that "Looks Back" at the previous row.
			-- Thus it retrieves the scroing_class from the previous season.
			LAG(SCORING_CLASS, 1) OVER (
				-- PARTITION BY: It is used to divide the player_name into partitions. 
				-- (same 'names' are grouped together and ordered by current_season)
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREVIOUS_SCORING_CLASS,
			-- It retrieves the is_active from the previous season.
			LAG(IS_ACTIVE, 1) OVER (
				-- (same 'names' are grouped together and ordered by current_season)
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREVIOUS_IS_ACTIVE
		FROM
            PLAYERS
    ),
    WITH_INDICATOR AS (
        SELECT
            *,
            CASE
                WHEN SCORING_CLASS <> PREVIOUS_SCORING_CLASS THEN 1
                WHEN IS_ACTIVE <> PREVIOUS_IS_ACTIVE THEN 1
                ELSE 0
            END AS CHANGE_INDICATOR
        FROM
            WITH_PREVIOUS
    ),
    WITH_STREAKS AS (
        SELECT
            *,
            SUM(CHANGE_INDICATOR) OVER (
                PARTITION BY PLAYER_NAME
                ORDER BY CURRENT_SEASON
            ) AS STREAK_IDENTIFIER
        FROM
            WITH_INDICATOR
    )
SELECT
	PLAYER_NAME,
	SCORING_CLASS,
    IS_ACTIVE,
	MIN(CURRENT_SEASON) AS START_SEASON,
	MAX(CURRENT_SEASON) AS END_SEASON
FROM
	WITH_STREAKS
WHERE
	STREAK_IDENTIFIER > 1
GROUP BY
	PLAYER_NAME,
	STREAK_IDENTIFIER,
	IS_ACTIVE,
	SCORING_CLASS





----------- SCD TABLE - FILTER AND INSERT INTO PLAYERS_SCD TABLE DATA------------

INSERT INTO PLAYERS_SCD
WITH
	WITH_PREVIOUS AS (
		SELECT
			PLAYER_NAME,
			CURRENT_SEASON,
			SCORING_CLASS,
			IS_ACTIVE,
			-- LAG() function is a window function that "Looks Back" at the previous row.
			-- Thus it retrieves the scroing_class from the previous season.
			LAG(SCORING_CLASS, 1) OVER (
				-- PARTITION BY: It is used to divide the player_name into partitions. 
				-- (same 'names' are grouped together and ordered by current_season)
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREVIOUS_SCORING_CLASS,
			-- It retrieves the is_active from the previous season.
			LAG(IS_ACTIVE, 1) OVER (
				-- (same 'names' are grouped together and ordered by current_season)
				PARTITION BY
					PLAYER_NAME
				ORDER BY
					CURRENT_SEASON
			) AS PREVIOUS_IS_ACTIVE
		FROM
            PLAYERS
		WHERE CURRENT_SEASON <= 2021
    ),
    WITH_INDICATOR AS (
        SELECT
            *,
            CASE
                WHEN SCORING_CLASS <> PREVIOUS_SCORING_CLASS THEN 1
                WHEN IS_ACTIVE <> PREVIOUS_IS_ACTIVE THEN 1
                ELSE 0
            END AS CHANGE_INDICATOR
        FROM
            WITH_PREVIOUS
    ),
    WITH_STREAKS AS (
        SELECT
            *,
            SUM(CHANGE_INDICATOR) OVER (
                PARTITION BY PLAYER_NAME
                ORDER BY CURRENT_SEASON
            ) AS STREAK_IDENTIFIER
        FROM
            WITH_INDICATOR
    )
SELECT
	PLAYER_NAME,
	IS_ACTIVE,
	SCORING_CLASS,
	MIN(CURRENT_SEASON) AS START_SEASON,
	MAX(CURRENT_SEASON) AS END_SEASON,
	2021 AS CURRENT_SEASON
FROM
	WITH_STREAKS
GROUP BY
	PLAYER_NAME,
	STREAK_IDENTIFIER,
	IS_ACTIVE,
	SCORING_CLASS

----- CREATING TYPE OF SCD TYPE -----

CREATE TYPE SCD_TYPE AS (
    SCORING_CLASS SCORING_CLASS,
	IS_ACTIVE BOOLEAN,
	START_SEASON INTEGER,
	END_SEASON INTEGER
)

-------- SCD TABLE  TYPE TWO --------
-- Show filters for:
    -- Last season's records
    -- Historical records (expired)
    -- Current season records
    -- Unchanged records
    -- New records
    
WITH
	LAST_SEASON_SCD AS (
		SELECT
			*
		FROM
			PLAYERS_SCD
		WHERE
			CURRENT_SEASON = 2021
			AND END_SEASON = 2021
	),
	HISTORICAL_SCD AS (
		SELECT
			PLAYER_NAME,
			SCORING_CLASS,
			IS_ACTIVE,
			START_SEASON,
			END_SEASON
		FROM
			PLAYERS_SCD
		WHERE
			CURRENT_SEASON = 2021
			AND END_SEASON < 2021
	),
	THIS_SEASON_DATA AS (
		SELECT
			*
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 2022
	),
	UNCHANGED_RECORDS AS (
		SELECT
			TS.PLAYER_NAME,
			TS.SCORING_CLASS,
			TS.IS_ACTIVE,
			LS.START_SEASON,
			TS.CURRENT_SEASON AS END_SEASON
		FROM
			THIS_SEASON_DATA TS
			JOIN LAST_SEASON_SCD LS ON TS.PLAYER_NAME = LS.PLAYER_NAME
		WHERE
			TS.SCORING_CLASS = LS.SCORING_CLASS
			AND TS.IS_ACTIVE = LS.IS_ACTIVE
	),
	CHANGED_RECORDS AS (
		SELECT
			TS.PLAYER_NAME,
			UNNEST(
				ARRAY[
					ROW (
						LS.SCORING_CLASS,
						LS.IS_ACTIVE,
						LS.START_SEASON,
						LS.END_SEASON
					)::SCD_TYPE,
					ROW (
						TS.SCORING_CLASS,
						TS.IS_ACTIVE,
						TS.CURRENT_SEASON,
						TS.CURRENT_SEASON
					)::SCD_TYPE
				]
			) AS RECORDS
		FROM
			THIS_SEASON_DATA TS
			LEFT JOIN LAST_SEASON_SCD LS ON TS.PLAYER_NAME = LS.PLAYER_NAME
		WHERE
			(TS.SCORING_CLASS <> LS.SCORING_CLASS)
			OR (TS.IS_ACTIVE <> LS.IS_ACTIVE)
	),
	UNNESTED_CHANGED_RECORDS AS (
		SELECT
			PLAYER_NAME,
			(RECORDS::SCD_TYPE).SCORING_CLASS,
			(RECORDS::SCD_TYPE).IS_ACTIVE,
			(RECORDS::SCD_TYPE).START_SEASON,
			(RECORDS::SCD_TYPE).END_SEASON
		FROM
			CHANGED_RECORDS
	),
	NEW_RECORDS AS (
		 SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL
	)
SELECT
	*, 2022 AS CURRENT_SEASON
FROM(
SELECT * FROM 
	HISTORICAL_SCD
UNION ALL
SELECT
	*
FROM
	UNCHANGED_RECORDS
UNION ALL
SELECT
	*
FROM
	UNNESTED_CHANGED_RECORDS
UNION ALL
SELECT
	*
FROM
	NEW_RECORDS ) RESULTS
	
