import pandas as pd
from dotenv import load_dotenv
import os
from googleapiclient.discovery import build

def extract_youtube_data():
    try:
        print("Starting YouTube data extraction...")
        load_dotenv()

        YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
        if not YOUTUBE_API_KEY:
            raise ValueError("YOUTUBE_API_KEY is not set")

        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

        channel_ids = [
            'UCYO_jab_esuFRV4b17AJtAw',  
            'UC8butISFwT-Wl7EV0hUK0BQ',  
            'UC7cs8q-gJRlGwj4A8OmCmXg'   
        ]

        def get_channels_statistics(youtube, channel_ids):
            all_data = []
            request = youtube.channels().list(
                part="snippet, contentDetails, statistics",
                id=','.join(channel_ids)
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    'ChannelName': item['snippet']['title'],
                    'subscribers': item['statistics']['subscriberCount'],
                    'views': item['statistics']['viewCount'],
                    'totalVideos': item['statistics']['videoCount'],
                    'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
                }
                all_data.append(data)
            return pd.DataFrame(all_data)

        channel_stats = get_channels_statistics(youtube, channel_ids)
        print(f"Channel statistics extracted: {channel_stats.shape[0]} channels")

        playlist_ids = channel_stats['playlistId'].tolist()

        def get_video_ids(youtube, playlist_ids):
            all_video_ids = []
            for playlist_id in playlist_ids:
                video_ids = []
                request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50
                )
                response = request.execute()

                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')
                while next_page_token:
                    request = youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    for item in response['items']:
                        video_ids.append(item['contentDetails']['videoId'])

                    next_page_token = response.get('nextPageToken')

                all_video_ids.extend(video_ids)

            print(f"Extracted {len(all_video_ids)} video IDs")
            return all_video_ids

        video_ids = get_video_ids(youtube, playlist_ids)

        def get_video_details(youtube, video_ids):
            all_video_info = []
            try:
                for i in range(0, len(video_ids), 50):
                    request = youtube.videos().list(
                        part='snippet,contentDetails,statistics',
                        id=','.join(video_ids[i:i+50])
                    )
                    response = request.execute()

                    for video in response.get('items', []):
                        video_statistics = {
                            'ChannelTitle': video['snippet']['channelTitle'],
                            'Title': video['snippet']['title'],
                            'Published_date': video['snippet']['publishedAt'],
                            'Views': video['statistics'].get('viewCount', 0),
                            'Likes': video['statistics'].get('likeCount', 0),
                            'Comments': video['statistics'].get('commentCount', 0)
                        }
                        all_video_info.append(video_statistics)
            except Exception as e:
                print(f"An error occurred: {str(e)}")
            
            print(f"Extracted data for {len(all_video_info)} videos")
            return pd.DataFrame(all_video_info)

        df_YT_data = get_video_details(youtube, video_ids)

        output_path = '/opt/airflow/data/Youtube_Data_Raw.csv'
        df_YT_data.to_csv(output_path, index=False)

        print(f"Data saved to {output_path}")
        return "Extraction Complete"

    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise

# extract_youtube_data()