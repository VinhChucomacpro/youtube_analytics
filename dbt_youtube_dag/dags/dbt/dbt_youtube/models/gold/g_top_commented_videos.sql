{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'comments']
    )
}}

SELECT
    v.video_id,
    v.video_title,
    v.video_published_at,
    
    -- 💬 Comment metrics
    COUNT(DISTINCT c.comment_id) as total_comments,
    AVG(c.like_count) as avg_comment_likes,
    SUM(c.reply_count) as total_replies,
    AVG(c.comment_length) as avg_comment_length,
    AVG(c.engagement_score) as avg_engagement_score,
    
    -- 🎭 Sentiment distribution
    SUM(CASE WHEN c.sentiment = 'POSITIVE' THEN 1 ELSE 0 END) as positive_comments,
    SUM(CASE WHEN c.sentiment = 'NEGATIVE' THEN 1 ELSE 0 END) as negative_comments,
    SUM(CASE WHEN c.sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) as neutral_comments,
    
    -- 📊 Sentiment percentage
    ROUND(SUM(CASE WHEN c.sentiment = 'POSITIVE' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as positive_rate,
    ROUND(SUM(CASE WHEN c.sentiment = 'NEGATIVE' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as negative_rate,
    
    -- 📈 Engagement metrics
    ROUND(COUNT(DISTINCT c.comment_id) * 100.0 / NULLIF(v.view_count, 0), 4) as comment_rate,
    
    -- 🎬 Video metrics
    v.view_count,
    v.like_count as video_likes,
    v.comment_count as api_comment_count,
    
    -- 📅 Time metrics
    DATEDIFF(day, v.video_published_at, CURRENT_TIMESTAMP()) as days_since_published

FROM {{ ref('stg_videos') }} v
LEFT JOIN {{ ref('stg_comments') }} c ON v.video_id = c.video_id

GROUP BY 
    v.video_id, 
    v.video_title, 
    v.video_published_at,
    v.view_count, 
    v.like_count,
    v.comment_count

HAVING COUNT(DISTINCT c.comment_id) > 0

ORDER BY total_comments DESC