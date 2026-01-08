{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'comments']
    )
}}

SELECT
    DATE_TRUNC('day', published_at) as comment_date,
    
    -- 📊 Daily volume
    COUNT(*) as daily_comments,
    COUNT(DISTINCT author_name) as unique_commenters,
    COUNT(DISTINCT video_id) as videos_with_comments,
    
    -- 👍 Engagement metrics
    AVG(like_count) as avg_likes,
    SUM(like_count) as total_likes,
    AVG(reply_count) as avg_replies,
    
    -- 🎭 Sentiment distribution
    SUM(CASE WHEN sentiment = 'POSITIVE' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) as neutral_count,
    
    -- 📊 Sentiment percentages
    ROUND(SUM(CASE WHEN sentiment = 'POSITIVE' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as positive_rate,
    
    -- 📝 Text metrics
    AVG(comment_length) as avg_comment_length,
    AVG(emoji_count) as avg_emojis,
    
    -- 🎯 Engagement score
    AVG(engagement_score) as avg_engagement_score

FROM {{ ref('stg_comments') }}

WHERE published_at IS NOT NULL

GROUP BY DATE_TRUNC('day', published_at)

ORDER BY comment_date DESC