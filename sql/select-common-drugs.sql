WITH cohort_drugs AS (
SELECT
    cohort.person_id,
    atc.ancestor_concept_name AS drug_category
FROM
    cohort AS cohort
    LEFT JOIN
    drug_exposure AS drug
    ON cohort.person_id = drug.person_id
    AND DATEDIFF(cohort.disease_start_date, drug.drug_exposure_start_date) BETWEEN 0 AND 360

    LEFT JOIN
    atc_categories AS atc
    ON condition.condition_concept_id = atc.descendant_concept_id
)
SELECT 
    person_id,
    drug_category
FROM 
    cohort_drugs
    INNER JOIN 
    (
        SELECT
            drug_category,
            COUNT(*) AS category_count
        FROM cohort_drugs
        GROUP BY drug_category
        ORDER BY category_count DESC
        LIMIT {num_top_categories}
    ) AS most_common_categories
    ON cohort_drugs.drug_category = most_common_categories.drug_category
;
