"""
Part 1: YouTube Data Collector

dependencies:
pip install google-api-python-client pandas psycopg2-binary python-dotenv
"""

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime
from dotenv import load_dotenv

class YouTubeDataCollector:
    def __init__(self, api_key):
        """ Initialize YouTube API Client"""
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.channel_data = []
        self.video_data = []
        self.comment_data = []
    
    def get_channel_id_from_username(self, username):
        """Get ID from username"""
        try:
            request = self.youtube.channels().list(
                part='id',
                forUsername=username
            )
            response = request.execute()
            
            if 'items' in response and response['items']:
                channel_id = response['items'][0]['id']
                print(f"✓ got channel ID: {channel_id}")
                return channel_id
            else:
                print(f"✗ cannot find username: {username}")
                return None
        except Exception as e:
            print(f"✗ got wrong channel ID: {e}")
            return None
    
    def get_channel_id_from_url(self, url):
        """get channel ID from URL"""
        import re
        
        # try different URLs
        patterns = [
            r'youtube\.com/channel/([^/\?]+)',  # /channel/UC...
            r'youtube\.com/c/([^/\?]+)',        # /c/username
            r'youtube\.com/user/([^/\?]+)',     # /user/username
            r'youtube\.com/@([^/\?]+)',         # /@handle
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                identifier = match.group(1)
                if identifier.startswith('UC'):
                    print(f"✓ extract ID from URL: {identifier}")
                    return identifier
                else:
                    print(f"finding username/handle: {identifier}")
                    return self.get_channel_id_from_username(identifier)
        
        print(f"✗ cannot found channel from url: {url}")
        return None
    
    def get_channel_stats(self, channel_id):
        """channel statistics"""
        try:
            request = self.youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
            )
            response = request.execute()
            
            # 检查响应是否有效
            if 'items' not in response:
                print(f"✗ Error API response: {response}")
                if 'error' in response:
                    print(f"✗ Error Details: {response['error']}")
                return None
            
            if not response['items']:
                print(f"✗ Cannot Found channel: {channel_id}")
                print(" Check channel ID")
                return None
            
            if response['items']:
                channel = response['items'][0]
                stats = {
                    'channel_id': channel_id,
                    'channel_name': channel['snippet']['title'],
                    'channel_description': channel['snippet']['description'],
                    'subscribers': int(channel['statistics'].get('subscriberCount', 0)),
                    'total_views': int(channel['statistics'].get('viewCount', 0)),
                    'total_videos': int(channel['statistics'].get('videoCount', 0)),
                    'country': channel['snippet'].get('country', 'N/A'),
                    'published_at': channel['snippet']['publishedAt'],
                    'uploads_playlist': channel['contentDetails']['relatedPlaylists']['uploads'],
                    'collected_at': datetime.now().isoformat()
                }
                self.channel_data.append(stats)
                print(f"✓ got channel information: {stats['channel_name']}")
                return stats
            return None
        except HttpError as e:
            print(f"✗ Error API HTTP: {e}")
            error_content = e.error_details if hasattr(e, 'error_details') else str(e)
            print(f"✗ Error Details: {error_content}")
            
            # 检查常见错误
            if 'quotaExceeded' in str(e):
                print("  → API Quota is up，waiting for tomorrow or use another API key")
            elif 'keyInvalid' in str(e) or 'API key not valid' in str(e):
                print("  → API key is invalid，check API key")
            elif 'API key expired' in str(e):
                print("  → API key is expired，renew API key")
            
            return None
        except Exception as e:
            print(f"✗ Unkown Error: {type(e).__name__}: {e}")
            return None
    
    def get_video_ids(self, uploads_playlist_id, max_results=50):
        """get ID from uploaded video list"""
        video_ids = []
        next_page_token = None
        
        print(f"get video ID (max {max_results} 个)...")
        
        while len(video_ids) < max_results:
            try:
                request = self.youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=uploads_playlist_id,
                    maxResults=min(50, max_results - len(video_ids)),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            except HttpError as e:
                print(f"✗ get veido ID Error: {e}")
                break
        
        print(f"✓ got {len(video_ids)} video ID")
        return video_ids
    
    def get_video_details(self, video_ids):
        """获取视频详细信息"""
        print(f"Getting {len(video_ids)} video detail information...")
        
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            try:
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(batch)
                )
                response = request.execute()
                
                for video in response['items']:
                    snippet = video['snippet']
                    stats = video['statistics']
                    content = video['contentDetails']
                    
                    video_data = {
                        'video_id': video['id'],
                        'channel_id': snippet['channelId'],
                        'title': snippet['title'],
                        'description': snippet['description'],
                        'published_at': snippet['publishedAt'],
                        'tags': ','.join(snippet.get('tags', [])),
                        'category_id': snippet.get('categoryId', 'N/A'),
                        'duration': content['duration'],
                        'definition': content['definition'],
                        'caption': content.get('caption', 'false'),
                        'view_count': int(stats.get('viewCount', 0)),
                        'like_count': int(stats.get('likeCount', 0)),
                        'comment_count': int(stats.get('commentCount', 0)),
                        'collected_at': datetime.now().isoformat()
                    }
                    self.video_data.append(video_data)
            except HttpError as e:
                print(f"✗ get video error information (batch {i//50 + 1}): {e}")
        
        print(f"✓ got {len(self.video_data)} video detailed information")
    
    def get_video_comments(self, video_id, max_comments=100):
        """获取单个视频的评论"""
        comments = []
        next_page_token = None
        
        try:
            while len(comments) < max_comments:
                request = self.youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=min(100, max_comments - len(comments)),
                    pageToken=next_page_token,
                    textFormat='plainText',
                    order='relevance'
                )
                response = request.execute()
                
                for item in response['items']:
                    comment_snippet = item['snippet']['topLevelComment']['snippet']
                    comments.append({
                        'video_id': video_id,
                        'comment_id': item['snippet']['topLevelComment']['id'],
                        'author': comment_snippet['authorDisplayName'],
                        'comment_text': comment_snippet['textDisplay'],
                        'like_count': comment_snippet['likeCount'],
                        'published_at': comment_snippet['publishedAt'],
                        'updated_at': comment_snippet.get('updatedAt', comment_snippet['publishedAt']),
                        'reply_count': item['snippet']['totalReplyCount'],
                        'collected_at': datetime.now().isoformat()
                    })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
        except HttpError as e:
            if 'commentsDisabled' in str(e):
                print(f"  Comment disabled: {video_id}")
            else:
                print(f"  Cannot Get Comment: {video_id} - {e}")
        
        return comments
    
    def collect_all_comments(self, max_comments_per_video=100, max_videos=None):
        """收集所有视频的评论"""
        videos_to_process = self.video_data[:max_videos] if max_videos else self.video_data
        total_videos = len(videos_to_process)
        
        print(f"\nGetting {total_videos} Video Comments...")
        
        for idx, video in enumerate(videos_to_process, 1):
            print(f"[{idx}/{total_videos}] Processing: {video['title'][:50]}...")
            comments = self.get_video_comments(video['video_id'], max_comments_per_video)
            self.comment_data.extend(comments)
            print(f"  ✓ Get {len(comments)} Comments")
        
        print(f"\n✓ Got {len(self.comment_data)} Comments in Total")
    
    def export_to_csv(self, output_dir='youtube_data'):
        """导出数据到CSV文件"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 导出频道数据
        if self.channel_data:
            df_channel = pd.DataFrame(self.channel_data)
            channel_file = f"{output_dir}/channel_data_{timestamp}.csv"
            df_channel.to_csv(channel_file, index=False, encoding='utf-8-sig')
            print(f"✓ Channel Data is Saved: {channel_file}")
        
        # 导出视频数据
        if self.video_data:
            df_videos = pd.DataFrame(self.video_data)
            video_file = f"{output_dir}/video_data_{timestamp}.csv"
            df_videos.to_csv(video_file, index=False, encoding='utf-8-sig')
            print(f"✓ video meta Data is saved: {video_file} ({len(df_videos)} records)")
        
        # 导出评论数据
        if self.comment_data:
            df_comments = pd.DataFrame(self.comment_data)
            comment_file = f"{output_dir}/comment_data_{timestamp}.csv"
            df_comments.to_csv(comment_file, index=False, encoding='utf-8-sig')
            print(f"✓ Comments data is saved: {comment_file} ({len(df_comments)} records)")
        
        return {
            'channel_file': channel_file if self.channel_data else None,
            'video_file': video_file if self.video_data else None,
            'comment_file': comment_file if self.comment_data else None
        }
    
    def export_to_postgres(self, db_config):
        """导出数据到PostgreSQL数据库"""
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                port=db_config.get('port', 5432)
            )
            cursor = conn.cursor()
            
            # 创建表
            self._create_tables(cursor)
            
            # 插入频道数据
            if self.channel_data:
                self._insert_channel_data(cursor, self.channel_data)
                print(f"✓ Inserted {len(self.channel_data)} channel data")
            
            # 插入视频数据
            if self.video_data:
                self._insert_video_data(cursor, self.video_data)
                print(f"✓ Inserted {len(self.video_data)} video data")
            
            # 插入评论数据
            if self.comment_data:
                self._insert_comment_data(cursor, self.comment_data)
                print(f"✓ Inserted {len(self.comment_data)} Comments data")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print("✓ Data Expoted to PostgreSQL")
            
        except Exception as e:
            print(f"✗ PostgreSQL Export Error: {e}")
    
    def _create_tables(self, cursor):
        """创建数据库表"""
        # 频道表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_channels (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(500),
                channel_description TEXT,
                subscribers BIGINT,
                total_views BIGINT,
                total_videos INTEGER,
                country VARCHAR(100),
                published_at TIMESTAMP,
                uploads_playlist VARCHAR(255),
                collected_at TIMESTAMP
            )
        """)
        
        # 视频表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_videos (
                video_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                title TEXT,
                description TEXT,
                published_at TIMESTAMP,
                tags TEXT,
                category_id VARCHAR(50),
                duration VARCHAR(50),
                definition VARCHAR(10),
                caption VARCHAR(10),
                view_count BIGINT,
                like_count INTEGER,
                comment_count INTEGER,
                collected_at TIMESTAMP,
                FOREIGN KEY (channel_id) REFERENCES youtube_channels(channel_id)
            )
        """)
        
        # 评论表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_comments (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255),
                author VARCHAR(500),
                comment_text TEXT,
                like_count INTEGER,
                published_at TIMESTAMP,
                updated_at TIMESTAMP,
                reply_count INTEGER,
                collected_at TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES youtube_videos(video_id)
            )
        """)
    
    def _insert_channel_data(self, cursor, data):
        """插入频道数据"""
        for channel in data:
            cursor.execute("""
                INSERT INTO youtube_channels 
                (channel_id, channel_name, channel_description, subscribers, total_views, 
                 total_videos, country, published_at, uploads_playlist, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET
                    subscribers = EXCLUDED.subscribers,
                    total_views = EXCLUDED.total_views,
                    total_videos = EXCLUDED.total_videos,
                    collected_at = EXCLUDED.collected_at
            """, (
                channel['channel_id'], channel['channel_name'], 
                channel['channel_description'], channel['subscribers'],
                channel['total_views'], channel['total_videos'],
                channel['country'], channel['published_at'],
                channel['uploads_playlist'], channel['collected_at']
            ))
    
    def _insert_video_data(self, cursor, data):
        """插入视频数据"""
        for video in data:
            cursor.execute("""
                INSERT INTO youtube_videos 
                (video_id, channel_id, title, description, published_at, tags, 
                 category_id, duration, definition, caption, view_count, 
                 like_count, comment_count, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count,
                    collected_at = EXCLUDED.collected_at
            """, (
                video['video_id'], video['channel_id'], video['title'],
                video['description'], video['published_at'], video['tags'],
                video['category_id'], video['duration'], video['definition'],
                video['caption'], video['view_count'], video['like_count'],
                video['comment_count'], video['collected_at']
            ))
    
    def _insert_comment_data(self, cursor, data):
        """插入评论数据"""
        for comment in data:
            cursor.execute("""
                INSERT INTO youtube_comments 
                (comment_id, video_id, author, comment_text, like_count, 
                 published_at, updated_at, reply_count, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (comment_id) DO NOTHING
            """, (
                comment['comment_id'], comment['video_id'], comment['author'],
                comment['comment_text'], comment['like_count'], 
                comment['published_at'], comment['updated_at'],
                comment['reply_count'], comment['collected_at']
            ))


# ============== Main ==============
def main():
    # loading .env file
    load_dotenv()
    
    # ===== Config Area =====
    API_KEY = os.getenv('YOUTUBE_API_KEY', 'YOUR_API_KEY_HERE')
    
    # Channel ID Config - pick one method:
    # method 1: channel ID (recommended if you have it)
    #CHANNEL_ID = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'  # eg.: Google Developers
    
    # method 2: Channel URL (get ID automatically)
    # CHANNEL_URL = 'https://www.youtube.com/@GoogleDevelopers'
    
    # method 3: Channel Username (get ID automatically)
    CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', 'GoogleDevelopers')
    
    # Check Configuration
    if API_KEY == 'YOUR_API_KEY_HERE':
        print("=" * 60)
        print("Error: Please Use a valid API KEY!")
        print("=" * 60)
        print("\nGet API key steps:")
        print("1. visit https://console.cloud.google.com/")
        print("2. Create a new Project")
        print("3. enable 'YouTube Data API v3'")
        print("4. create credentials > API KEY")
        print("5. copy API Key to API_KEY in .env file or in Main config YOUR_API_KEY_HERE")
        print("=" * 60)
        return
    
    print(f"\nPrompts: Current USING the API KEY Prefix: {API_KEY[:10]}...")
    
    # 处理频道ID
    collector = YouTubeDataCollector(API_KEY)
    
    # 如果提供了URL，尝试提取频道ID
    if 'CHANNEL_URL' in locals():
        CHANNEL_ID = collector.get_channel_id_from_url(CHANNEL_URL)
        if not CHANNEL_ID:
            return
    # 如果提供了用户名，获取频道ID
    elif 'CHANNEL_USERNAME' in locals():
        CHANNEL_ID = collector.get_channel_id_from_username(CHANNEL_USERNAME)
        if not CHANNEL_ID:
            return
    
    print(f"Prompts: Target Channel ID: {CHANNEL_ID}\n")
    
    # PostgreSQL配置 (可选)
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'youtube_data'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'your_password'),
        'port': os.getenv('DB_PORT', 5432)
    }
    
    # data collection parameters
    MAX_VIDEOS = os.getenv('MAX_VIDEOS', '50')  # maximum video to fetch
    MAX_COMMENTS_PER_VIDEO = os.getenv('MAX_COMMENTS_PER_VIDEO', '100')  # maximum comments per video
    MAX_VIDEOS_FOR_COMMENTS = os.getenv('MAX_VIDEOS_FOR_COMMENTS', '20')  # maximum videos to fetch comments from
    
    # ===== 开始数据收集 =====
    print("=" * 60)
    print("YouTube Data Collector")
    print("=" * 60)
    
    # 1. 获取频道信息
    print("\n[Steps 1/5] Channel data Getting...")
    channel_stats = collector.get_channel_stats(CHANNEL_ID)
    
    if not channel_stats:
        print("\n" + "=" * 60)
        print("✗ cannot get channel information!")
        print("=" * 60)
        print("\nReasons:")
        print("1. API KEY is invalid or expired")
        print("2. Channel ID is wrong")
        print("3. API quota exceeded")
        print("4. Network issues")
        print("\nSolutions:")
        print("→ Check if API KEY is Correct")
        print("→ Check channel ID")
        print("→ Check API Quota: https://console.cloud.google.com/apis/api/youtube.googleapis.com/quotas")
        print("=" * 60)
        return
    
    # 2. 获取视频ID列表
    print("\n[Step 2/5] getting video lists...")
    video_ids = collector.get_video_ids(
        channel_stats['uploads_playlist'], 
        max_results=MAX_VIDEOS
    )
    
    # 3. 获取视频详细信息
    print("\n[Step 3/5] Get video Detail Information...")
    collector.get_video_details(video_ids)
    
    # 4. 收集评论
    print("\n[Step 4/5] Collecting Comments on Videos...")
    collector.collect_all_comments(
        max_comments_per_video=MAX_COMMENTS_PER_VIDEO,
        max_videos=MAX_VIDEOS_FOR_COMMENTS
    )
    
    # 5. 导出数据
    print("\n[Step 5/5] Export Data...")
    
    # 导出到CSV
    print("\n>>> Export to CSV File...")
    files = collector.export_to_csv()
    
    # 导出到PostgreSQL (可选 - 取消注释以使用)
    # print("\n>>> 导出到 PostgreSQL...")
    # collector.export_to_postgres(DB_CONFIG)
    
    # 总结
    print("\n" + "=" * 60)
    print("Youtube Data Collection is Done！")
    print("=" * 60)
    print(f"Channel: {channel_stats['channel_name']}")
    print(f"Video Number: {len(collector.video_data)}")
    print(f"Commnet Number: {len(collector.comment_data)}")
    print("\nNext Step: Run Sentiment Analysis on Comments")
    print("=" * 60)


if __name__ == "__main__":
    main()