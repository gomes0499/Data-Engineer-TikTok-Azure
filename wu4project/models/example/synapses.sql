WITH clean_synapses AS (
    SELECT
        user_id,
        video_id,
        TRY_CONVERT(float, JSON_VALUE(confidence_scores, '$.positive')) as positive_confidence_score,
        TRY_CONVERT(float, JSON_VALUE(confidence_scores, '$.neutral')) as neutral_confidence_score,
        TRY_CONVERT(float, JSON_VALUE(confidence_scores, '$.negative')) as negative_confidence_score,
        sentiment,
        created_at
    FROM
        events
)

SELECT
    *,
    CASE
        WHEN positive_confidence_score >= neutral_confidence_score AND positive_confidence_score >= negative_confidence_score THEN 'Positive'
        WHEN neutral_confidence_score >= positive_confidence_score AND neutral_confidence_score >= negative_confidence_score THEN 'Neutral'
        ELSE 'Negative'
    END AS sentiment_label
FROM
    clean_synapses
