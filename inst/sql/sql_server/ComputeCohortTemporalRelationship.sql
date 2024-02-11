-- Start of Common Table Expression (CTE) named event_series
WITH RECURSIVE event_series(subject_id, target_start_date, event_episode_start_date, event_episode_end_date, 
                            treatment_date, target_cohort_definition_id, event_cohort_definition_id, day_count) AS (
    -- Initial SELECT statement (anchor member of the recursive CTE)
    SELECT 
        CAST(t.subject_id AS BIGINT) subject_id,  -- The ID of the subject (patient)
        CAST(t.cohort_start_date AS DATE) AS target_start_date, -- Start date of the target cohort as a date
        CAST(e.cohort_start_date AS DATE) AS event_episode_start_date, -- Start date of the event cohort as a date
        CAST(e.cohort_end_date AS DATE) AS event_episode_end_date, -- End date of the event cohort as a date
        CAST(e.cohort_start_date AS DATE) AS treatment_date, -- The initial treatment date set as the start date of the event cohort
        CAST(t.cohort_definition_id AS BIGINT) AS target_cohort_definition_id, -- The definition ID of the target cohort, cast to BIGINT
        CAST(e.cohort_definition_id AS BIGINT) AS event_cohort_definition_id, -- The definition ID of the event cohort, cast to BIGINT   
        CAST(1 AS BIGINT) day_count -- Starting day count as 1 for each subject's treatment day
    
    FROM @cohort_database_schema.@cohort_table t -- The target cohort table
    INNER JOIN @cohort_database_schema.@cohort_table e -- The event cohort table
        ON t.subject_id = e.subject_id -- Joining the tables on subject_id to match patients
        -- AND e.cohort_end_date < t.cohort_start_date -- Filtering to consider only events that ended before the target cohort started
        AND 
        ((DATEDIFF(day, t.cohort_start_date, e.cohort_start_date)) < @max_days_diff 
              OR
         (DATEDIFF(day, e.cohort_start_date, t.cohort_start_date)) < @max_days_diff 
        ) -- Filtering for events that started within a year before the target cohort
    WHERE t.cohort_definition_id IN (@target_cohort_definition_ids)
          AND e.cohort_definition_id IN (@event_cohort_definition_ids)

    UNION ALL

    -- Recursive member of the CTE that iterates through the event series
    SELECT 
        subject_id,
        target_start_date,
        event_episode_start_date,
        event_episode_end_date,
        CAST(treatment_date + INTERVAL '1' DAY AS DATE), -- Incrementing the treatment date by one day in each iteration
        target_cohort_definition_id, -- Propagating the target cohort definition ID through the recursion
        event_cohort_definition_id, -- Propagating the event cohort definition ID through the recursion
        day_count + 1
    FROM event_series
    WHERE treatment_date < event_episode_end_date -- Continue the recursion until the treatment date reaches the end of the event episode
)
-- Selecting from the recursive CTE
SELECT 
    target_cohort_definition_id, -- Including the target cohort definition ID in the final output
    event_cohort_definition_id, -- Including the event cohort definition ID in the final output
    CEIL(DATEDIFF(day, target_start_date, treatment_date)/(@round_days*1.0)) AS treatment_period, -- Grouping by weekly periods
    COUNT(DISTINCT subject_id) AS target_subjects, -- Counting the distinct number of subjects for each treatment period
    COUNT(*) AS target_events, -- Counting the total number of events for each treatment period
    AVG(day_count) AS avg_treatment_days_per_subject, -- Average treatment days per subject
    MIN(day_count) AS min_treatment_days_per_subject, -- Minimum treatment days per subject
    MAX(day_count) AS max_treatment_days_per_subject, -- Maximum treatment days per subject
    SUM(day_count) AS total_treatment_days -- Total treatment days across all subjects
FROM event_series
GROUP BY CEIL(DATEDIFF(day, target_start_date, treatment_date)/(@round_days*1.0)), target_cohort_definition_id, event_cohort_definition_id -- Grouping results by the weekly period and both definition IDs
ORDER BY CEIL(DATEDIFF(day, target_start_date, treatment_date)/(@round_days*1.0)); -- Ordering the results by the weekly period
