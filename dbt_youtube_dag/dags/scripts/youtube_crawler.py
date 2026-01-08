from googleapiclient.discovery import build
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Optional
import snowflake.connector

# API Key từ biến môi trường
API_KEY = os.getenv('YOUTUBE_API_KEY', 'AIzaSyDmJRwO5FX80wz34R43gzW_cykWZZ062qI')
CHANNEL_ID = "UClyA28-01x4z60eWQ2kiNbA"
SEEDS_PATH = "/usr/local/airflow/dags/dbt/dbt_youtube/seeds"

# Snowflake connection
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'YOUTUBE_DEV_SCHEMA')
}


def get_snowflake_connection():
    """Tạo kết nối Snowflake"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def get_last_crawl_time(entity_type: str, entity_id: str) -> Optional[datetime]:
    """Lấy thời điểm crawl cuối cùng từ Snowflake"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT last_crawl_at 
        FROM CRAWL_METADATA 
        WHERE entity_type = %s AND entity_id = %s
        ORDER BY last_crawl_at DESC
        LIMIT 1
        """
        cursor.execute(query, (entity_type, entity_id))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        print(f"⚠️  Error getting last crawl time: {e}")
        return None


def update_crawl_metadata(entity_type: str, entity_id: str, items_count: int, status: str = 'SUCCESS', error: str = None):
    """Cập nhật metadata crawl vào Snowflake"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        crawl_id = f"{entity_type}_{entity_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        query = """
        INSERT INTO CRAWL_METADATA (
            crawl_id, entity_type, entity_id, last_crawl_at, 
            items_crawled, status, error_message, updated_at
        )
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP(), %s, %s, %s, CURRENT_TIMESTAMP())
        """
        cursor.execute(query, (crawl_id, entity_type, entity_id, items_count, status, error))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Updated crawl metadata: {entity_type} - {entity_id}")
    except Exception as e:
        print(f"⚠️  Error updating metadata: {e}")


def save_video_history(video_stats_df: pd.DataFrame):
    """Lưu lịch sử thay đổi stats vào Snowflake"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        for _, row in video_stats_df.iterrows():
            history_id = f"{row['video_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            query = """
            INSERT INTO VIDEO_HISTORY (
                history_id, video_id, view_count, like_count, 
                comment_count, snapshot_at
            )
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """
            cursor.execute(query, (
                history_id,
                row['video_id'],
                int(row.get('view_count', 0)),
                int(row.get('like_count', 0)),
                int(row.get('comment_count', 0))
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Saved {len(video_stats_df)} video history records")
    except Exception as e:
        print(f"⚠️  Error saving video history: {e}")


def clean_text_columns(df):
    """Làm sạch text columns"""
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
            df[col] = df[col].str.replace(r'[\n\r\t]+', ' ', regex=True)
            df[col] = df[col].str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
            df[col] = df[col].replace('nan', '')
    return df


def fetch_channel_info(youtube, channel_id):
    """
    Lấy thông tin channel
    Match với br_channels.sql
    """
    request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    response = request.execute()
    
    items = response.get('items', [])
    if not items:
        return pd.DataFrame()
    
    item = items[0]
    snippet = item['snippet']
    statistics = item['statistics']
    thumbnails = snippet.get('thumbnails', {})
    
    data = [{
        'channel_id': channel_id,
        'title': snippet.get('title', ''),
        'customUrl': snippet.get('customUrl', ''),
        'publishedAt': snippet.get('publishedAt', ''),
        'country': snippet.get('country', ''),
        'subscriberCount': int(statistics.get('subscriberCount', 0)),
        'viewCount': int(statistics.get('viewCount', 0)),
        'videoCount': int(statistics.get('videoCount', 0)),
        'thumbnail_high': thumbnails.get('high', {}).get('url', '')
    }]
    
    return pd.DataFrame(data)


def fetch_playlists(youtube, channel_id):
    """
    Lấy danh sách playlists
    Match với br_playlists.sql: playlist_id, playlist_title, playlist_desc, published_at, channel_id, thumb_high, item_count
    """
    playlists = []
    next_page_token = None
    
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        
        for item in response.get('items', []):
            snippet = item['snippet']
            content_details = item['contentDetails']
            thumbnails = snippet.get('thumbnails', {})
            
            playlists.append({
                'playlist_id': item['id'],
                'playlist_title': snippet.get('title', ''),  # ✅ MATCH br_playlists
                'playlist_desc': snippet.get('description', ''),  # ✅ MATCH br_playlists
                'published_at': snippet.get('publishedAt', ''),  # ✅ MATCH br_playlists
                'channel_id': channel_id,
                'thumb_high': thumbnails.get('high', {}).get('url', ''),  # ✅ MATCH br_playlists
                'item_count': content_details.get('itemCount', 0)  # ✅ MATCH br_playlists
            })
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return pd.DataFrame(playlists)


def fetch_playlist_items(youtube, playlist_ids):
    """
    Lấy playlist items
    Match với br_playlist_items.sql: id, playlist_id, video_id, videoPublishedAt, position, publishedAt
    """
    items = []
    
    for playlist_id in playlist_ids:
        next_page_token = None
        
        while True:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get('items', []):
                snippet = item['snippet']
                content_details = item['contentDetails']
                
                items.append({
                    'id': item['id'],  # ✅ MATCH br_playlist_items
                    'playlist_id': playlist_id,
                    'video_id': content_details['videoId'],  # ✅ FIX: snake_case
                    'videoPublishedAt': content_details.get('videoPublishedAt', ''),  # ✅ MATCH br_playlist_items
                    'position': snippet.get('position', 0),
                    'publishedAt': snippet.get('publishedAt', '')  # ✅ MATCH br_playlist_items
                })
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    
    return pd.DataFrame(items)


def fetch_video_stats(youtube, video_ids):
    """
    Lấy thống kê videos
    Match với br_video_stats.sql: video_id, video_title, video_desc, video_published_at, duration_iso8601, 
                                    view_count, like_count, comment_count, live_actual_start, live_actual_end, live_scheduled_start
    """
    stats = []
    
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails,liveStreamingDetails",
            id=','.join(batch)
        )
        response = request.execute()
        
        for item in response.get('items', []):
            snippet = item['snippet']
            statistics = item['statistics']
            content_details = item['contentDetails']
            live_details = item.get('liveStreamingDetails', {})
            
            stats.append({
                'video_id': item['id'],
                'video_title': snippet.get('title', ''),
                'video_desc': snippet.get('description', ''),
                'video_published_at': snippet.get('publishedAt', ''),
                'duration_iso8601': content_details.get('duration', ''),  # ✅ MATCH br_video_stats
                'view_count': int(statistics.get('viewCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0)),
                'live_actual_start': live_details.get('actualStartTime', ''),  # ✅ MATCH br_video_stats
                'live_actual_end': live_details.get('actualEndTime', ''),  # ✅ MATCH br_video_stats
                'live_scheduled_start': live_details.get('scheduledStartTime', '')  # ✅ MATCH br_video_stats
            })
    
    return pd.DataFrame(stats)


def fetch_video_comments(youtube, video_ids, max_comments_per_video: int = 100):
    """
    Crawl comments từ videos
    Match với br_video_comments.sql
    """
    all_comments = []
    
    for video_id in video_ids:
        try:
            next_page_token = None
            comments_count = 0
            
            while comments_count < max_comments_per_video:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(100, max_comments_per_video - comments_count),
                    pageToken=next_page_token,
                    order='relevance'
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    top_comment = item['snippet']['topLevelComment']
                    snippet = top_comment['snippet']
                    
                    all_comments.append({
                        'comment_id': top_comment['id'],
                        'video_id': video_id,
                        'author_name': snippet.get('authorDisplayName', ''),
                        'author_channel_id': snippet.get('authorChannelId', {}).get('value', ''),
                        'comment_text': snippet.get('textDisplay', ''),
                        'like_count': int(snippet.get('likeCount', 0)),
                        'published_at': snippet.get('publishedAt', ''),
                        'updated_at': snippet.get('updatedAt', ''),
                        'reply_count': item['snippet'].get('totalReplyCount', 0)
                    })
                    comments_count += 1
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token or comments_count >= max_comments_per_video:
                    break
            
            print(f"✅ Crawled {comments_count} comments from video {video_id}")
            
        except Exception as e:
            print(f"⚠️  Error crawling comments from {video_id}: {e}")
            continue
    
    return pd.DataFrame(all_comments)


def crawl_youtube_data(use_cdc: bool = True, crawl_comments: bool = True):
    """
    Crawl dữ liệu YouTube với CDC và comments
    """
    print(f"--- Bắt đầu thu thập dữ liệu cho kênh {CHANNEL_ID} ---")
    print(f"CDC enabled: {use_cdc}")
    print(f"Crawl comments: {crawl_comments}")
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    # 🔍 CDC: Lấy thời điểm crawl cuối
    last_crawl = None
    if use_cdc:
        last_crawl = get_last_crawl_time('videos', CHANNEL_ID)
        if last_crawl:
            print(f"📅 Last crawl: {last_crawl}")
        else:
            print("📅 First time crawl - getting all data")
    
    # 1. Channel info
    print("\n1. Fetching Channel Info...")
    channel_data = fetch_channel_info(youtube, CHANNEL_ID)
    print(f"   Columns: {channel_data.columns.tolist()}")
    
    # 2. Playlists
    print("\n2. Fetching Playlists...")
    playlists_data = fetch_playlists(youtube, CHANNEL_ID)
    print(f"   Columns: {playlists_data.columns.tolist()}")
    
    # 3. Playlist items
    print("\n3. Fetching Playlist Items...")
    playlist_items_data = fetch_playlist_items(youtube, playlists_data['playlist_id'].tolist())
    print(f"   Columns: {playlist_items_data.columns.tolist()}")
    
    # 4. Video stats
    print("\n4. Fetching Video Stats...")
    video_ids = playlist_items_data['video_id'].unique().tolist()
    video_stats_data = fetch_video_stats(youtube, video_ids)
    print(f"   Columns: {video_stats_data.columns.tolist()}")
    
    # 🆕 5. Lưu lịch sử thay đổi
    if use_cdc and not video_stats_data.empty:
        save_video_history(video_stats_data)
    
    # 🆕 6. Comments
    comments_data = pd.DataFrame()
    if crawl_comments and not video_stats_data.empty:
        print("\n5. Fetching Comments...")
        # Lấy top 20 videos có nhiều view nhất
        top_videos = video_stats_data.nlargest(20, 'view_count')['video_id'].tolist()
        comments_data = fetch_video_comments(youtube, top_videos, max_comments_per_video=100)
        if not comments_data.empty:
            print(f"   Columns: {comments_data.columns.tolist()}")
    
    # Clean data - ✅ THÊM ESCAPE COMMAS
    channel_data = clean_text_columns(channel_data)
    playlists_data = clean_text_columns(playlists_data)
    video_stats_data = clean_text_columns(video_stats_data)
    playlist_items_data = clean_text_columns(playlist_items_data)
    if not comments_data.empty:
        comments_data = clean_text_columns(comments_data)
    
    # Debug
    print("\n=== SUMMARY ===")
    print(f"Channels: {len(channel_data)} rows")
    print(f"Playlists: {len(playlists_data)} rows")
    print(f"Playlist Items: {len(playlist_items_data)} rows")
    print(f"Video Stats: {len(video_stats_data)} rows")
    if not comments_data.empty:
        print(f"Comments: {len(comments_data)} rows")
    
    # Tạo thư mục seeds
    os.makedirs(SEEDS_PATH, exist_ok=True)
    
    # ✅ SỬA: QUOTE_MINIMAL để handle commas trong text
    csv_params = {
        'index': False,
        'quoting': 1,  # ✅ QUOTE_MINIMAL - Quote khi có commas, newlines
        'doublequote': True,
        'lineterminator': '\n',
        'encoding': 'utf-8'
    }
    
    channel_data.to_csv(f"{SEEDS_PATH}/channels.csv", **csv_params)
    print(f"✅ Saved: channels.csv")
    
    playlists_data.to_csv(f"{SEEDS_PATH}/playlists.csv", **csv_params)
    print(f"✅ Saved: playlists.csv")
    
    video_stats_data.to_csv(f"{SEEDS_PATH}/video_stats.csv", **csv_params)
    print(f"✅ Saved: video_stats.csv")
    
    playlist_items_data.to_csv(f"{SEEDS_PATH}/playlist_items.csv", **csv_params)
    print(f"✅ Saved: playlist_items.csv")
    
    if not comments_data.empty:
        comments_data.to_csv(f"{SEEDS_PATH}/video_comments.csv", **csv_params)
        print(f"✅ Saved: video_comments.csv")
    
    # 🔍 Update metadata
    if use_cdc:
        update_crawl_metadata('videos', CHANNEL_ID, len(video_stats_data))
        if not comments_data.empty:
            update_crawl_metadata('comments', CHANNEL_ID, len(comments_data))
    
    print("\n--- Hoàn thành thu thập dữ liệu ---")
    return True


if __name__ == "__main__":
    crawl_youtube_data(use_cdc=True, crawl_comments=True)