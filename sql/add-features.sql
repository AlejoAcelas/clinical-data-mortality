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
    condition_occurrence AS condition
    ON cohort.person_id = condition.person_id
    AND DATEDIFF(cohort.disease_start_date, condition.condition_start_date) BETWEEN 0 AND 360

    LEFT JOIN
    drug_exposure AS drug
    ON cohort.person_id = drug.person_id
    AND DATEDIFF(cohort.disease_start_date, drug.drug_exposure_start_date) BETWEEN 0 AND 360
;
