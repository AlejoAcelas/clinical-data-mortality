SELECT
    CASE
        WHEN cohort.death_date IS NULL THEN 0
        ELSE 1
    END AS dead,
    person.gender_concept_id,
    person.race_concept_id,
    person.ethnicity_concept_id,
    person.year_of_birth,
    condition.condition_concept_id,
    drug.drug_concept_id
FROM
    cohort AS cohort
    LEFT JOIN 
    person AS person
    ON cohort.person_id = person.person_id

    LEFT JOIN
    (
        SELECT
            person_id,
            
    )
    
;
