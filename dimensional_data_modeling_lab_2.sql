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