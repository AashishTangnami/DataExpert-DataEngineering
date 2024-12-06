/*

Additive Dimensions:
    - No double counting.

A dimension is additive over a specific window of time
if and only if the grain of dat over that window can only ever be one value at a time.

- You don't need to use  COUNT(DISTINCT) on preaggregated dimensions.
- Remember non-additive dimensions are usually only non-additive with respect to COUNT aggregations but not SUM aggregations.

When do we use enums:
    - Low-to-meduim cardinality columns (enums < 50).
     - Built in data quality
     - Bulit in static fields
     - Build in documentation

Enumerations and SubPartitions:
   - Chunking data into smaller partitions.


Thrift is a way to manage schema in logging as well as ETL and shares enum, schema accross the board.


Use of ENUM PATTERN in data modeling:
    - UNIT ECONOMICS (
        - FEES, INCOME, EXPENSES
        - COUPOUNS, DISCOUNTS, REBATES
        - CREDITS, DEBITS, REFUNDS
        - TAXES, DUTIES, LEVIES
    )
    - INFRASTRUCTURE GRAPH (
        - APPLICATIONS, 
        - DATABASES,
        - CI/CD TOOLS,
        )
    - FAMILY OF APPS (
        - FB,
        - INSTAGRAM,
        - TWITTER ETC
    )  

MODEL DATA FROM DISPARATE SOURCES INTO A SHARED SCHEMA:
 -- FLEXIBLE SCHEMA --

BENEFITS OF FLEXIBLE SCHEMA:
    - NO NEED TO RUN ALTER TABLE COMMANDS
    - CAN MANAGE LOTS OF COLUMNS
    - NOT MANY NULLS COLUMNS
    - RARELY USED COLUMNS - OTHER PROPERTIES 

DRAWBACKS:
    - COMPRESSION IS USUALLY WORSE. (JSON, MAPS)
    - READABLITIY, QUERABILITY IS WORSE.


*/
------ GRAPH DATA MODELING ------
/*
    Graph Data Modeling:
        Graph modeling is relationship focused NOT enitity focused.
        Usually the model looks like
            - Identifier : String
            - Type : String
            - Properties : Map<String, Object>


    The relationships are modeled a little bit more in depth:
       - Subject_identifier : String
       - subject_type : Vertex_type
       - Object_identifier : String
       - object_type : Vertex_type
       - edge_type : Edge_type
       - properties : Map<String, Object>

    
*/

--- GRAPH DATA MODELING ---
--- VERTICES TYPE---
CREATE TYPE VERTEX_TYPE AS ENUM('player', 'team', 'game');

CREATE TABLE VERTICES (
	IDENTIFIER TEXT,
	TYPE VERTEX_TYPE,
	PROPERTIES JSON,
	PRIMARY KEY (IDENTIFIER, TYPE)
)


-- Create an enumeration type for different types of edges in the graph
CREATE TYPE EDGE_TYPE AS ENUM(
    'plays_against', 
    'shares_team',
    'plays_in',
    'plays_on'
);

-- Create the EDGES table to represent relationships (edges) between vertices in the graph
CREATE TABLE EDGES (
    SUBJECT_IDENTIFIER TEXT,    -- Identifier of the subject vertex
    SUBJECT_TYPE VERTEX_TYPE,   -- Type of the subject vertex (from VERTEX_TYPE)
    OBJECT_IDENTIFIER TEXT,     -- Identifier of the object vertex
    OBJECT_TYPE VERTEX_TYPE,    -- Type of the object vertex (from VERTEX_TYPE)
    EDGE_TYPE EDGE_TYPE,        -- Type of edge connecting the vertices
    PROPERTIES JSON,            -- Additional properties of the edge stored in JSON format
    PRIMARY KEY (
        SUBJECT_IDENTIFIER,
        SUBJECT_TYPE,
        OBJECT_IDENTIFIER,
        OBJECT_TYPE,
        EDGE_TYPE
    )
);

-------

-- Insert data into the vertices table from the GAMES table
INSERT INTO vertices
SELECT
    GAME_ID AS IDENTIFIER,                -- Unique identifier for each game vertex
    'game'::VERTEX_TYPE AS TYPE,          -- Explicitly cast type as 'game' vertex
    JSON_BUILD_OBJECT(                    -- Build a JSON object with game properties
        'pts_home', PTS_HOME,             -- Points scored by the home team
        'pts_away', PTS_AWAY,             -- Points scored by the away team
        'winning_team',                   -- Determine the winning team's ID
        CASE
            WHEN HOME_TEAM_WINS = 1 THEN HOME_TEAM_ID   -- If home team wins
            ELSE VISITOR_TEAM_ID                        -- If visitor team wins
        END
    ) AS PROPERTIES
FROM
    GAMES;  -- Source table containing game data



INSERT INTO vertices
SELECT
	GAME_ID AS IDENTIFIER,
	'game'::VERTEX_TYPE AS TYPE,
	JSON_BUILD_OBJECT(
		'pts_home',
		PTS_HOME,
		'pts_away',
		PTS_AWAY,
		'winning_team',
		CASE
			WHEN HOME_TEAM_WINS = 1 THEN HOME_TEAM_ID
			ELSE VISITOR_TEAM_ID
		END
	) AS PROPERTIES
FROM
	GAMES;



-- Aggregate player data using a Common Table Expression (CTE)
-- And Insert into the 'vertices' table
INSERT INTO VERTICES
WITH PLAYERS_AGG AS (
    SELECT
        PLAYER_ID AS IDENTIFIER,              -- Use PLAYER_ID as the unique identifier
        MAX(PLAYER_NAME) AS PLAYER_NAME,      -- Get the player's name (assuming it's consistent)
        COUNT(1) AS NUMBER_OF_GAMES,          -- Total number of games the player has participated in
        SUM(PTS) AS TOTAL_POINTS,             -- Sum of points scored by the player
        ARRAY_AGG(DISTINCT TEAM_ID) AS TEAMS  -- Array of distinct teams the player has played for
    FROM
        GAME_DETAILS
    GROUP BY
        PLAYER_ID                             -- Group by PLAYER_ID to aggregate data per player
)
-- Select and format the aggregated player data
SELECT
    IDENTIFIER,                               -- Player's unique identifier
    'player'::VERTEX_TYPE,                    -- Cast 'player' as the vertex type
    JSON_BUILD_OBJECT(                        -- Build a JSON object containing player properties
        'player_name', PLAYER_NAME,
        'number_of_games', NUMBER_OF_GAMES,
        'total_points', TOTAL_POINTS,
        'teams', TEAMS
    )
FROM
    PLAYERS_AGG;    

---- -- Insert unique team records into the 'vertices' table
INSERT INTO VERTICES
-- De-duplicateD teams using a Common Table Expression (CTE) 
WITH TEAMS_DEDUPTED AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                TEAM_ID                -- Partition by TEAM_ID to identify duplicates
        ) AS ROW_NUM                   -- Assign row numbers to each partition
    FROM
        TEAMS                          -- Source table containing team data
)
SELECT
    TEAM_ID AS IDENTIFIER,             -- Unique identifier for each team vertex
    'team'::VERTEX_TYPE AS TYPE,       -- Cast 'team' as the vertex type
    JSON_BUILD_OBJECT(                 -- Build a JSON object with team properties
        'abbreviation', ABBREVIATION,
        'nickname', NICKNAME,
        'city', CITY,
        'arena', ARENA,
        'year_founded', YEARFOUNDED
    ) AS PROPERTIES
FROM
    TEAMS_DEDUPTED                     -- Use the deduplicated teams CTE
WHERE
    ROW_NUM = 1;                       -- Select only the first occurrence of each team


---------------------

-- Use a Common Table Expression (CTE) to deduplicate records based on PLAYER_ID
INSERT INTO EDGES
WITH DEDUPED AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                PLAYER_ID             -- Partition data by PLAYER_ID to group records per player
        ) AS ROW_NUM                   -- Assign a sequential row number within each partition
    FROM
        GAME_DETAILS                   -- Source table containing game details
)

-- Select and format data to create edges from players to games
SELECT
    PLAYER_ID AS SUBJECT_IDENTIFIER,        -- Player's unique identifier (subject of the edge)
    'player'::VERTEX_TYPE AS SUBJECT_TYPE,  -- Specify the vertex type as 'player' for the subject
    GAME_ID AS OBJECT_IDENTIFIER,           -- Game's unique identifier (object of the edge)
    'game'::VERTEX_TYPE AS OBJECT_TYPE,     -- Specify the vertex type as 'game' for the object
    'plays_in'::EDGE_TYPE AS EDGE_TYPE,     -- Define the edge type as 'plays_in' to represent the relationship
    JSON_BUILD_OBJECT(                      -- Build a JSON object to store edge properties
        'start_position', START_POSITION,       -- Add player's starting position in the game
        'pts', PTS,                             -- Add points scored by the player in the game
        'team_id', TEAM_ID,                     -- Include the team ID the player was part of
        'team_abbreviation', TEAM_ABBREVIATION  -- Include the team's abbreviation
    ) AS PROPERTIES
FROM
    DEDUPED
WHERE
    ROW_NUM = 1                              -- Select only the first record per player to avoid duplicates


---------------------
SELECT
	V.PROPERTIES ->> 'player_name',
	MAX(cast(E.PROPERTIES ->> 'pts' as integer))
FROM
	VERTICES V
	JOIN EDGES E ON E.SUBJECT_IDENTIFIER = V.IDENTIFIER
	AND E.SUBJECT_TYPE = V.TYPE
GROUP BY
	1
ORDER BY
	2 DESC


----

----- SELF JOIN AND INSERT QUERY USING CTE WITH WINDOW FUNCTION----
-- Insert edges between players based on their game interactions
INSERT INTO EDGES
-- First CTE: Deduplicate game details to ensure one record per player per game
WITH DEDUPED AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                PLAYER_ID,
                GAME_ID
        ) AS ROW_NUM                     -- Assign row numbers within player-game groups
    FROM
        GAME_DETAILS
),
-- Second CTE: Filter to keep only unique player-game combinations
FILTERED AS (
    SELECT
        *
    FROM
        DEDUPED
    WHERE
        ROW_NUM = 1                      -- Keep only first occurrence
),
-- Third CTE: Aggregate player interactions and calculate statistics
AGGREGATED AS (
    SELECT
        F1.PLAYER_ID AS SUBJECT_PLAYER_ID,   -- First player in the relationship
        F2.PLAYER_ID AS OBJECT_PLAYER_ID,    -- Second player in the relationship
        CASE
            WHEN F1.TEAM_ABBREVIATION = F2.TEAM_ABBREVIATION 
            THEN 'shares_team'::EDGE_TYPE     -- Players on same team
            ELSE 'plays_against'::EDGE_TYPE   -- Players on opposing teams
        END AS EDGE_TYPE,
        MAX(F1.PLAYER_NAME) AS SUBJECT_PLAYER_NAME,    -- Name of first player
        MAX(F2.PLAYER_NAME) AS OBJECT_PLAYER_NAME,     -- Name of second player
        COUNT(1) AS NUM_GAMES,                         -- Number of games played together/against
        SUM(F1.PTS) AS LEFT_POINTS,                   -- Points scored by first player
        SUM(F2.PTS) AS RIGHT_POINTS                   -- Points scored by second player
    FROM
        FILTERED F1
        JOIN FILTERED F2 ON F1.GAME_ID = F2.GAME_ID   -- Match players in same game
        AND F1.PLAYER_NAME <> F2.PLAYER_NAME          -- Exclude self-matches
    WHERE
        F1.PLAYER_ID > F2.PLAYER_ID                   -- Prevent duplicate relationships
    GROUP BY
        F1.PLAYER_ID,
        F2.PLAYER_ID,
        CASE
            WHEN F1.TEAM_ABBREVIATION = F2.TEAM_ABBREVIATION 
            THEN 'shares_team'::EDGE_TYPE
            ELSE 'plays_against'::EDGE_TYPE
        END
)
-- Final SELECT: Format data for insertion into EDGES table
SELECT
    SUBJECT_PLAYER_ID AS SUBJECT_IDENTIFIER,          -- Source player ID
    'player'::VERTEX_TYPE AS SUBJECT_TYPE,            -- Vertex type for source
    OBJECT_PLAYER_ID AS OBJECT_IDENTIFIER,            -- Target player ID
    'player'::VERTEX_TYPE AS OBJECT_TYPE,             -- Vertex type for target
    EDGE_TYPE AS EDGE_TYPE,                           -- Relationship type
    JSON_BUILD_OBJECT(                                -- Create JSON of edge properties
        'num_games', NUM_GAMES,                       -- Number of games together
        'subject_points', LEFT_POINTS,                -- Points by source player
        'object_points', RIGHT_POINTS                 -- Points by target player
    ) AS PROPERTIES
FROM
    AGGREGATED;


SELECT
    -- Player name from JSON properties
    V.PROPERTIES ->> 'player_name',
    
    -- Connected player's identifier
    E.OBJECT_IDENTIFIER,
    
    -- Calculate average points per game
    CAST(V.PROPERTIES ->> 'number_of_games' AS REAL) /   -- Number of games played
    CASE
        WHEN CAST(V.PROPERTIES ->> 'total_points' AS REAL) = 0 THEN 1  -- Avoid division by zero
        ELSE CAST(V.PROPERTIES ->> 'total_points' AS REAL)             -- Total career points
    END,
    
    -- Points scored against this specific opponent
    E.PROPERTIES ->> 'subject_points',
    
    -- Number of games played against this opponent
    E.PROPERTIES ->> 'num_games'
FROM
    VERTICES V
    JOIN EDGES E ON V.IDENTIFIER = E.SUBJECT_IDENTIFIER
        AND V.TYPE = E.SUBJECT_TYPE
WHERE
    E.OBJECT_TYPE = 'player'::VERTEX_TYPE;