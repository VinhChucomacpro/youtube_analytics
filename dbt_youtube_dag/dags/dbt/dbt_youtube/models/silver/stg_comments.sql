{{ config(materialized='table', tags=['silver', 'comments']) }}

WITH cleaned_comments AS (
    SELECT
        comment_id,
        video_id,
        author_name,
        author_channel_id,
        TRIM(REGEXP_REPLACE(comment_text, '\\s+', ' ')) as comment_text,
        CASE 
            WHEN LOWER(comment_text) LIKE '%hay%' 
                OR LOWER(comment_text) LIKE '%đỉnh%'
                OR LOWER(comment_text) LIKE '%tuyệt%'
                OR LOWER(comment_text) LIKE '%amazing%'
                OR LOWER(comment_text) LIKE '%love%'
                OR LOWER(comment_text) LIKE '%great%'
                OR LOWER(comment_text) LIKE '%excellent%'
                OR LOWER(comment_text) LIKE '%❤%'
                OR LOWER(comment_text) LIKE '%🔥%' THEN 'POSITIVE'
            WHEN LOWER(comment_text) LIKE '%dở%'
                OR LOWER(comment_text) LIKE '%tệ%'
                OR LOWER(comment_text) LIKE '%kém%'
                OR LOWER(comment_text) LIKE '%bad%'
                OR LOWER(comment_text) LIKE '%hate%'
                OR LOWER(comment_text) LIKE '%terrible%'
                OR LOWER(comment_text) LIKE '%👎%' THEN 'NEGATIVE'
            ELSE 'NEUTRAL'
        END as sentiment,
        like_count,
        -- ✅ Loại bỏ TYPEOF và CASE - chỉ sử dụng trực tiếp
        published_at,
        updated_at,
        reply_count,
        REGEXP_COUNT(comment_text, '@\\w+') as mention_count,
        LENGTH(comment_text) as comment_length,
        LENGTH(comment_text) - LENGTH(REGEXP_REPLACE(comment_text, '[😀-🙏🌀-🗿🚀-🛿]', '')) as emoji_count,
        REGEXP_COUNT(comment_text, '\\d+') as number_count
    FROM {{ ref('br_video_comments') }}
    WHERE comment_text IS NOT NULL
      AND comment_text != ''
      AND comment_text != 'nan'
)

SELECT 
    *,
    CASE 
        WHEN comment_length > 0 THEN (like_count * 10 + reply_count * 5 + mention_count * 2) / comment_length
        ELSE 0
    END as engagement_score
FROM cleaned_comments