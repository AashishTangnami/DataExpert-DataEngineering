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