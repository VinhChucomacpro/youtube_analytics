{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'comments']
    )
}}

SELECT
    sentiment,
    
    -- 📊 Count metrics
    COUNT(*) as comment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    
    -- 👍 Like metrics
    AVG(like_count) as avg_likes,
    SUM(like_count) as total_likes,
    MAX(like_count) as max_likes,
    
    -- 💬 Reply metrics
    AVG(reply_count) as avg_replies,
    SUM(reply_count) as total_replies,
    
    -- 📝 Text metrics
    AVG(comment_length) as avg_length,
    MAX(comment_length) as max_length,
    MIN(comment_length) as min_length,
    
    -- 😊 Emoji metrics
    AVG(emoji_count) as avg_emojis,
    
    -- 🎯 Engagement metrics
    AVG(engagement_score) as avg_engagement_score,
    MAX(engagement_score) as max_engagement_score

FROM {{ ref('stg_comments') }}

GROUP BY sentiment

ORDER BY comment_count DESC