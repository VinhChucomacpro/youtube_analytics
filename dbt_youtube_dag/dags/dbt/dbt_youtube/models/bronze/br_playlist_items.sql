{{ config(materialized='view', tags=['bronze']) }}

select
    id AS playlist_item_id,  -- ✅ SỬA: id thay vì playlistitem_id
    playlist_id,
    video_id,
    videoPublishedAt::timestamp AS "VIDEO_PUBLISHED_AT",  -- ✅ MATCH CSV
    position::int AS "POSITION",
    publishedAt::timestamp AS "ADDED_AT"  -- ✅ MATCH CSV
from {{ ref('playlist_items') }}