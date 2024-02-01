WITH selected_condition AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
        -- condition_end_date,
        -- condition_end_reason
    FROM 
        condition_occurrence
    WHERE
        condition_concept_id = 320128
)
SELECT
    sc.person_id,
    sc.condition_concept_id,
    sc.condition_start_date,
    CASE
        WHEN d.death_date IS NULL THEN 1
        ELSE 0
    END AS dead
FROM
    selected_condition AS sc
    INNER JOIN 
    observation_period AS op
    ON sc.person_id = op.person_id    
    AND DATEDIFF(sc.condition_start_date, op.observation_period_start_date) > 360
    AND DATEDIFF(op.observation_period_end_date, sc.condition_start_date) > 360

    LEFT JOIN
    death as d
    ON sc.person_id = d.person_id
    AND DATEDIFF(sc.condition_start_date, d.death_date) > 360
;



