{{
    config(
        materialized='table',
        tags=['bronze', 'comments']
    )
}}

-- Raw copy từ seed video_comments
SELECT
    comment_id,
    video_id,
    author_name,
    author_channel_id,
    comment_text,
    like_count,
    published_at,
    updated_at,
    reply_count
FROM {{ ref('video_comments') }}