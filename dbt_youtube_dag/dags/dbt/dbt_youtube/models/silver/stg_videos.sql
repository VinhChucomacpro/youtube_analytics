{{ config(materialized='view' ,
    tags=['silver']) }}

select
  pi.playlist_item_id,
  pi.playlist_id,
  pl.channel_id,                -- lấy từ playlists
  pi.video_id,
  s.video_title,
  s.video_desc,
  s.video_published_at,
  s.view_count,
  s.like_count,
  s.comment_count,
  s.duration_seconds,
  datediff('day', s.video_published_at, current_timestamp()) as "VIDEO_AGE_DAYS",
  case 
    when s.live_scheduled_start is not null then 'Live'
    when s.duration_seconds < 60 then 'Shorts'
    when s.duration_seconds is null then 'Private videos'
    else 'Normal videos'
  end as "VIDEO_TYPE",
  case
    when duration_seconds <   60 then '0-1m'
    when duration_seconds <  300 then '1-5m'
    when duration_seconds <  900 then '5-15m'
    when duration_seconds < 1800 then '15-30m'
    when duration_seconds < 3600 then '30-60m'
    else '60m+'
  end as "DURATION_BUCKET",
  pi.added_at              as "VIDEO_ADDED_TO_PLAYLIST_AT",   -- từ br_playlist_items
  pi.position              as "VIDEO_POSITION_IN_PLAYLIST",   -- từ br_playlist_items
  null                     as "VIDEO_THUMBNAIL_URL"           -- playlist_items không có thumb
from {{ ref('br_playlist_items') }} pi
left join {{ ref('br_playlists') }} pl   on pi.playlist_id = pl.playlist_id
left join {{ ref('br_video_stats') }} s  on pi.video_id = s.video_id