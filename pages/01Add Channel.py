import streamlit as st
import requests
import altair as alt
import pandas as pd
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os

# Set page configuration and enable dark theme for Altair plots
st.set_page_config(
    page_title="YouTube Dashboard",
    page_icon="D:\\Projects\\yt_data_analysis\\images\\ytlogo.png",
    layout="wide",
    initial_sidebar_state="auto"
)
alt.themes.enable("dark")


load_dotenv()

# Function to fetch channel details from YouTube Data API
def get_channel_details(api_key, channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,status,statistics&id={channel_id}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    channel_data = {
        "Channel ID": channel_id,
        "Channel Name": data["items"][0]["snippet"]["title"],
        "Description": data["items"][0]["snippet"]["description"],
        "Subscriber Count": data["items"][0]["statistics"]["subscriberCount"],
        "Total Videos": data["items"][0]["statistics"]["videoCount"],
        "Total Views": data["items"][0]["statistics"]["viewCount"],
        "Channel Status": data["items"][0]['status']['privacyStatus']
    }
    return channel_data

# Function to fetch video details from YouTube Data API
def get_video_details(api_key, channel_id):
    videos_data = []
    next_page_token = None

    while True:
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&key={api_key}&order=date&maxResults=50"
        if next_page_token:
            url += f"&pageToken={next_page_token}"
        response = requests.get(url)
        data = response.json()
        
        for item in data.get("items", []):
            if "id" in item and "videoId" in item["id"]:
                video_id = item["id"]["videoId"]
               
                video_title = item["snippet"]["title"]
                video_description = item["snippet"]["description"]
                video_published_at = item["snippet"]["publishedAt"]
                
                # Fetch video statistics
                video_stats_url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics,contentDetails&id={video_id}&key={api_key}"
                stats_response = requests.get(video_stats_url)
                stats_data = stats_response.json()
                if stats_data["items"]:
                    stats = stats_data["items"][0]["statistics"]
                    duration = stats_data["items"][0]["contentDetails"]["duration"]
                else:
                    stats = {"viewCount": "Not Available", "likeCount": "Not Available", "commentCount": "Not Available"}
                    
                video_data = {
                    "Video ID": video_id,
                    "Channel ID": channel_id,
                    "Title": video_title,
                    "Description": video_description,
                    "Views": stats.get("viewCount", "Not Available"),
                    "Likes": stats.get("likeCount", "Not Available"),
                    "Total Comments": stats.get("commentCount", "Not Available"),
                    "Duration":duration,
                    "Published Date & Time":video_published_at
                }
                videos_data.append(video_data)
            
        if "nextPageToken" in data:
            next_page_token = data["nextPageToken"]
        else:
            break
            
    return videos_data

# Streamlit web app
def main():
    st.title("YouTube Data Harvesting")
    
    
    channel_id = st.text_input("Enter a YouTube Channel ID:", placeholder="Paste YouTube ID here...")

    if st.button("Fetch Details"):
        if not channel_id:
            st.error("Please enter the YouTube Channel ID.")
        else:
            api_key = os.getenv("API_KEY")
            
            try:
                 # Fetch channel details
                st.write("### Channel Details")
                channel_data = get_channel_details(api_key, channel_id)
                st.dataframe(channel_data, width=500)
                st.write("### Video Details")
                with st.spinner("Fetching video details..."):
                    video_data = get_video_details(api_key, channel_id)
                    video_df = pd.DataFrame(video_data)
                    st.dataframe(video_df, width=1200, height=400)
                
                
                # Save channel and video data to database
                save_channel_details_to_database(channel_data, video_data)
                # print(video_data)
               
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.error("Please check the Channel ID.")

    # else:
    #     welcome_message()

# Function to save channel details and video data to the database
def save_channel_details_to_database(channel_data, video_data):
    
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASS"),  # Replace 'YOUR_PASSWORD' with your actual MySQL password
            database=os.getenv("DATABASE"),
            auth_plugin='mysql_native_password'
        )
        
        cursor = connection.cursor()

        # Check if the channel already exists in the database
        cursor.execute("SELECT * FROM channel WHERE channel_id = %s", (channel_data["Channel ID"],))
        existing_channel = cursor.fetchone()

        # If the channel exists, update the record; otherwise, insert a new record
        if existing_channel:
            update_channel_query = """
                UPDATE channel
                SET channel_name = %s, description = %s, subscriber_count = %s, total_videos = %s, total_views = %s, channel_status = %s
                WHERE channel_id = %s
            """
            channel_values = (
                channel_data["Channel Name"],
                channel_data["Description"],
                channel_data["Subscriber Count"],
                channel_data["Total Videos"],
                channel_data["Total Views"],
                channel_data["Channel Status"],
                channel_data["Channel ID"]
            )
            cursor.execute(update_channel_query, channel_values)
        else:
            insert_channel_query = """
                INSERT INTO channel (channel_id, channel_name, description, subscriber_count, total_videos, total_views, channel_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            channel_values = (
                channel_data["Channel ID"],
                channel_data["Channel Name"],
                channel_data["Description"],
                channel_data["Subscriber Count"],
                channel_data["Total Videos"],
                channel_data["Total Views"],
                channel_data["Channel Status"]
            )
            cursor.execute(insert_channel_query, channel_values)

        # Consume any unread result set
        for result in cursor.stored_results():
            result.fetchall()

        # Check if each video already exists in the database
        for video in video_data:
            # Convert ISO 8601 datetime string to MySQL datetime format
            published_at = datetime.strptime(video["Published Date & Time"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute("SELECT * FROM video WHERE video_id = %s", (video["Video ID"],))
            existing_video = cursor.fetchone()

            # If the video exists, update the record; otherwise, insert a new record
            if existing_video:
                update_video_query = """
                    UPDATE video
                    SET title = %s, description = %s, views = %s, likes = %s, total_comments = %s, duration = %s, published_at = %s
                    WHERE video_id = %s
                """
                video_values = (
                    video["Title"],
                    video["Description"],
                    video["Views"],
                    int(video["Likes"]) if video["Likes"] != "Not Available" else None,  
                    int(video["Total Comments"]) if video["Total Comments"] != "Not Available" else None, 
                    video["Duration"],
                    published_at,
                    video["Video ID"]
                )
                cursor.execute(update_video_query, video_values)
            else:
                insert_video_query = """
                    INSERT INTO video (video_id, channel_id, title, description, views, likes, total_comments, duration, published_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                video_values = (
                    video["Video ID"],
                    video["Channel ID"],
                    video["Title"],
                    video["Description"],
                    video["Views"],
                    int(video["Likes"]) if video["Likes"] != "Not Available" else None,  
                    int(video["Total Comments"]) if video["Total Comments"] != "Not Available" else None, 
                    video["Duration"],
                    published_at  # Use the converted datetime value
                )
                cursor.execute(insert_video_query, video_values)

        # Commit the transaction after processing all videos
        connection.commit()



        # Consume any unread result set
        for result in cursor.stored_results():
            result.fetchall()

        connection.commit()
        st.success("Data saved successfully.")


# Function to display welcome message
# def welcome_message():
#     st.write("""
#         # Welcome to YouTube Data Harvesting App
#         This application allows you to access and analyze data from multiple YouTube channels.
#         Please enter a YouTube channel ID in the sidebar to get started!
#     """)
    # st.image("analytics.gif", use_column_width=True, caption="Data Analytics in Action")
    

if __name__ == "__main__":
    main()
