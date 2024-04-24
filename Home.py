import streamlit as st

st.set_page_config(
    page_title="Youtube Analytics",
    page_icon="images/ytlogo.png"
)

st.write("""
        # Welcome to YouTube Data Harvesting App
        This application allows you to access and analyze data from multiple YouTube channels.
        Please enter a YouTube channel ID in the sidebar to get started!
    """)
st.image("images/analytics.gif", use_column_width=True, caption="Data Analytics in Action")
