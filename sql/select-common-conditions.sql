WITH cohort_conditions AS (
SELECT
    cohort.person_id,
    icd10.ancestor_concept_name AS condition_category
FROM
    cohort AS cohort
    LEFT JOIN
    condition_occurrence AS condition
    ON cohort.person_id = condition.person_id
    AND DATEDIFF(cohort.disease_start_date, condition.condition_start_date) BETWEEN 0 AND 360

    LEFT JOIN
    icd10_categories AS icd10
    ON condition.condition_concept_id = icd10.descendant_concept_id
)
SELECT 
    person_id,
    condition_category
FROM 
    cohort_conditions
    INNER JOIN 
    (
        SELECT
            condition_category,
            COUNT(*) AS category_count
        FROM cohort_conditions
        GROUP BY condition_category
        ORDER BY category_count DESC
        LIMIT {num_top_categories}
    ) AS most_common_categories
    ON cohort_conditions.condition_category = most_common_categories.condition_category
;
