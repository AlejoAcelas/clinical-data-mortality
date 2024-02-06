SELECT
    concept.concept_id AS ancestor_concept_id,
    concept.concept_name AS ancestor_concept_name,
    ancestor.descendant_concept_id
FROM
    concept_ancestor AS ancestor
    INNER JOIN
    concept
    ON ancestor.ancestor_concept_id = concept.concept_id
    AND concept.vocabulary_id = 'ATC'
    AND concept.concept_class_id = 'ATC 2nd'
    AND concept.domain_id = 'Drug'
;