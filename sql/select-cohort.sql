WITH selected_condition AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date
    FROM 
        condition_occurrence
    WHERE
        condition_concept_id = 320128
)
SELECT
    sc.person_id,
    sc.condition_concept_id AS disease_concept_id,
    sc.condition_start_date AS disease_start_date,
    d.death_date
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




