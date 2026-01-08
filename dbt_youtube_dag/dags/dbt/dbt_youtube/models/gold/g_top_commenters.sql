{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'comments']
    )
}}

SELECT
    author_name,
    author_channel_id,
    
    -- 💬 Comment activity
    COUNT(DISTINCT comment_id) as total_comments,
    COUNT(DISTINCT video_id) as videos_commented,
    
    -- 👍 Engagement received
    SUM(like_count) as total_likes_received,
    AVG(like_count) as avg_likes_per_comment,
    SUM(reply_count) as total_replies_received,
    
    -- 📝 Text behavior
    AVG(comment_length) as avg_comment_length,
    SUM(emoji_count) as total_emojis_used,
    SUM(mention_count) as total_mentions,
    
    -- 🎭 Sentiment pattern
    SUM(CASE WHEN sentiment = 'POSITIVE' THEN 1 ELSE 0 END) as positive_comments,
    SUM(CASE WHEN sentiment = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_comments,
    SUM(CASE WHEN sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) as neutral_comments,
    
    -- 📅 Time metrics
    MIN(published_at) as first_comment_at,
    MAX(published_at) as last_comment_at,
    DATEDIFF(day, MIN(published_at), MAX(published_at)) as active_days,
    
    -- 🎯 Engagement score
    AVG(engagement_score) as avg_engagement_score

FROM {{ ref('stg_comments') }}

GROUP BY author_name, author_channel_id

HAVING COUNT(DISTINCT comment_id) >= 2  -- Chỉ lấy users comment >= 2 lần

ORDER BY total_comments DESC

LIMIT 100