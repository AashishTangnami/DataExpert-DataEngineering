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


