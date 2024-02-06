SELECT
    features.*,
    icd10_chapters.ancestor_concept_name AS condition_category,
    atc.ancestor_concept_name AS drug_category
FROM
    features AS features
    LEFT JOIN
    icd10_categories AS icd10
    ON features.condition_concept_id = icd10.descendant_concept_id

    LEFT JOIN
    atc_categories AS atc
    ON features.drug_concept_id = atc.descendant_concept_id
;