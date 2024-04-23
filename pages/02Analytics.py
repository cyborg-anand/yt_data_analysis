import streamlit as st
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# Function to execute SQL queries
def execute_query(query, params=None):
    try:
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASS"), 
            database=os.getenv("DATABASE"),
            auth_plugin='mysql_native_password'
        )

        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        connection.commit()
        return result

    except mysql.connector.Error as error:
        st.error(f"Error executing query: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to get all channel names from the database
def get_all_channel_names():
    query = "SELECT channel_name FROM channel;"
    results = execute_query(query)
    return [result[0] for result in results]

# Streamlit web app
def main():
    st.title("YouTube Data Analysis")

    # Retrieve all channel names from the database
    channel_names = get_all_channel_names()

    # Select multiple channels
    selected_channels = st.multiselect("Select Channels:", channel_names)

    if st.button("Fetch Data"):
        if not selected_channels:
            st.error("Please select at least one channel.")
        else:
            # Query 1: Names of all videos and their corresponding channels
            st.header("1. Names of all videos and their corresponding channels")
            query_1 = "SELECT video.title AS 'Video Title', channel.channel_name AS 'Channel Name' FROM video JOIN channel ON video.channel_id = channel.channel_id WHERE channel.channel_name IN (%s);"
            query_1 = query_1 % ','.join(['%s'] * len(selected_channels))
            result_1 = execute_query(query_1, tuple(selected_channels))
            df_1 = pd.DataFrame(result_1, columns=["Video Title", "Channel Name"])
            st.dataframe(df_1)

            # Query 2: Channels with the most number of videos and their total counts
            st.header("2. Channels with the most number of videos and their total counts")
            query_2 = "SELECT channel_name, total_videos FROM channel WHERE channel_name IN (%s);"
            query_2 = query_2 % ','.join(['%s'] * len(selected_channels))
            result_2 = execute_query(query_2, tuple(selected_channels))
            df_2 = pd.DataFrame(result_2, columns=["Channel Name", "Total Videos"])
            st.dataframe(df_2)
            
            # Query 3: Top 10 most viewed videos and their respective channels
            st.header("3. Top 10 most viewed videos and their respective channels")
            query_3 = """
                SELECT v.title AS 'Video Title', c.channel_name AS 'Channel Name', v.views AS 'Views'
                FROM video v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE c.channel_name IN (%s)
                ORDER BY v.views DESC
                LIMIT 10;
            """
            query_3 = query_3 % ','.join(['%s'] * len(selected_channels))
            result_3 = execute_query(query_3, tuple(selected_channels))
            df_3 = pd.DataFrame(result_3, columns=["Video Title", "Channel Name", "Views"])
            st.dataframe(df_3)

             # Query 4: Number of comments made on each video and their corresponding video names
            st.header("4. Number of comments made on each video and their corresponding video names")
            query_4 = """
                SELECT v.title AS 'Video Title', v.total_comments AS 'Number of Comments'
                FROM video v
                WHERE v.channel_id IN (
                    SELECT channel_id FROM channel WHERE channel_name IN (%s)
                );
            """
            query_4 = query_4 % ','.join(['%s'] * len(selected_channels))
            result_4 = execute_query(query_4, tuple(selected_channels))
            df_4 = pd.DataFrame(result_4, columns=["Video Title", "Number of Comments"])
            st.dataframe(df_4)

            # Query 5: Videos with the highest number of likes and their corresponding channel names
            st.header("5. Videos with the highest number of likes and their corresponding channel names")
            query_5 = """
                SELECT v.title AS 'Video Title', v.likes AS 'Number of Likes', c.channel_name AS 'Channel Name'
                FROM video v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE v.channel_id IN (
                    SELECT channel_id FROM channel WHERE channel_name IN (%s)
                )
                ORDER BY v.likes DESC;
            """
            query_5 = query_5 % ','.join(['%s'] * len(selected_channels))
            result_5 = execute_query(query_5, tuple(selected_channels))
            df_5 = pd.DataFrame(result_5, columns=["Video Title", "Number of Likes", "Channel Name"])
            st.dataframe(df_5)

            #Query % (alt)
            
            # st.header("5. Video with the highest number of likes from each selected channel")
            # query_5 = """
            #     SELECT v.title AS 'Video Title', v.likes AS 'Number of Likes', c.channel_name AS 'Channel Name'
            #     FROM (
            #         SELECT *, ROW_NUMBER() OVER(PARTITION BY video.channel_id ORDER BY likes DESC) AS video_rank
            #         FROM video
            #         WHERE video.channel_id IN (
            #             SELECT channel_id FROM channel WHERE channel_name IN (%s)
            #         )
            #     ) v
            #     JOIN channel c ON v.channel_id = c.channel_id
            #     WHERE v.video_rank = 1
            #     ORDER BY v.likes DESC;
            # """
            # query_5 = query_5 % ','.join(['%s'] * len(selected_channels))
            # result_5 = execute_query(query_5, tuple(selected_channels))
            # df_5 = pd.DataFrame(result_5, columns=["Video Title", "Number of Likes", "Channel Name"])
            # st.dataframe(df_5)
            # Query 5: Total number of likes for each video and their corresponding video names
            # Query 5: Total number of likes for each video and their corresponding video names
            # Query 5: Total number of likes for each video and their corresponding video names
            st.header("6. Total number of likes for each video and their corresponding video names")
            query_6 = """
                SELECT v.title AS 'Video Title', v.likes AS 'Total Likes'
                FROM video v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE c.channel_name IN (%s);
            """
            query_6 = query_6 % ','.join(['%s'] * len(selected_channels))
            result_6 = execute_query(query_6, tuple(selected_channels))
            df_6 = pd.DataFrame(result_6, columns=["Video Title", "Total Likes"])
            st.dataframe(df_6)


            # Query 7: Total number of views for each channel and their corresponding channel names
            st.header("7. Total number of views for each channel and their corresponding channel names")
            query_7 = "SELECT channel_name AS 'Channel Name', total_views AS 'Total Views' FROM channel WHERE channel_name IN (%s);"
            query_7 = query_7 % ','.join(['%s'] * len(selected_channels))
            result_7 = execute_query(query_7, tuple(selected_channels))
            df_7 = pd.DataFrame(result_7, columns=["Channel Name", "Total Views"])
            st.dataframe(df_7)

            # Query 8: Names of selected channels that have published videos in the year 2022
            st.header("8. Names of selected channels that have published videos in the year 2022")
            query_8 = """
                SELECT DISTINCT c.channel_name AS 'Channel Name'
                FROM video v
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE YEAR(v.published_at) = 2022
                AND c.channel_name IN (%s);
            """
            query_8 = query_8 % ','.join(['%s'] * len(selected_channels))
            result_8 = execute_query(query_8, tuple(selected_channels))
            df_8 = pd.DataFrame(result_8, columns=["Channel Name"])
            st.dataframe(df_8)

            # Query 9: Average duration of all videos in each selected channel and their corresponding channel names
            # Sample durations from the 'video' table for selected channels
            st.header("Average duration of videos for selected channels:")
            
            # Initialize list to store results for each channel
            channel_results = []
            
            # Loop through each selected channel
            for channel_name in selected_channels:
                # Query to fetch durations for the current channel
                query_check_duration = """
                    SELECT duration
                    FROM video v
                    JOIN channel c ON v.channel_id = c.channel_id
                    WHERE c.channel_name = %s
                """
                result_check_duration = execute_query(query_check_duration, (channel_name,))
            
                total_duration_seconds = 0
                total_videos = 0
            
                # Calculate total duration and count of valid videos for the current channel
                for row in result_check_duration:
                    duration = row[0]
                    if duration:
                        # Parse duration in ISO 8601 format and calculate total duration in seconds
                        duration_components = duration[2:].split('M')
                        try:
                            hours = int(duration_components[0][:-1])
                        except ValueError:
                            hours = 0
                        try:
                            minutes = int(duration_components[1][:-1]) if len(duration_components) > 1 else 0
                        except ValueError:
                            minutes = 0
                        total_duration_seconds += hours * 3600 + minutes * 60
                        total_videos += 1
            
                # Calculate average duration for the current channel
                if total_videos > 0:
                    average_duration_seconds = total_duration_seconds / total_videos
                else:
                    average_duration_seconds = 0
            
                # Convert average duration to ISO 8601 format
                average_duration_iso8601 = f"PT{int(average_duration_seconds // 3600)}H{int((average_duration_seconds % 3600) // 60)}M{int(average_duration_seconds % 60)}S"
            
                # Append results for the current channel to the list
                channel_results.append({
                    "Channel Name": channel_name,
                    "Total Videos": total_videos,
                    "Total Duration (Seconds)": total_duration_seconds,
                    "Average Duration (ISO 8601)": average_duration_iso8601
                })
            
            # Create DataFrame from the list of results
            df_results = pd.DataFrame(channel_results)
            
            # Display DataFrame
            st.dataframe(df_results)

            






           







if __name__ == "__main__":
    main()
