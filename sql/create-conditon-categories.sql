SELECT
    concept.concept_id AS ancestor_concept_id,
    concept.concept_name AS ancestor_concept_name,
    ancestor.descendant_concept_id
FROM
    concept_ancestor AS ancestor
    INNER JOIN
    concept
    ON ancestor.ancestor_concept_id = concept.concept_id
    AND concept.vocabulary_id = 'ICD10'
    AND concept.concept_class_id = 'ICD10 Hierarchy'
    AND concept.domain_id = 'Condition'
;