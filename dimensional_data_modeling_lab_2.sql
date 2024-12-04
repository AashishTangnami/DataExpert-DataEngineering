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


--- CREATING TYPE 2 TABLE ---
--- SLOWLY CHANGING DIMENSION TYPE 2 TABLE---
CREATE TABLE PLAYERS_SCD (
	PLAYER_NAME TEXT,
	SCORING_CLASS SCORING_CLASS,
	IS_ACTIVE BOOLEAN,
	CURRENT_SEASON INTEGER,
	START_SEASON INTEGER,  -- start_date
	END_SEASON INTEGER -- end_date
	PRIMARY KEY (PLAYER_NAME, CURRENT_SEASON)
)



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