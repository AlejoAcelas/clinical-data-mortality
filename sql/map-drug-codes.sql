SELECT
    icd10.concept_name
FROM
    features AS features
    LEFT JOIN
    icd10_chapters AS icd10
    ON features.condition_concept_id = icd10.descendant_concept_id
