SELECT
    c.person_id,
    c.dead,
    p.gender_concept_id,
    p.race_concept_id,
    p.ethnicity_concept_id,
    p.year_of_birth,
    p.location_id
FROM
    cohort as c
    JOIN 
    person as p
    ON c.person_id = p.person_id
;