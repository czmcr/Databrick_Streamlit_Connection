import os
import streamlit as st
from databricks import sql
from dotenv import load_dotenv
import pandas as pd

def load_env_vars():
    load_dotenv()
    access_token = os.getenv("ACCESS_TOKEN")
    host = os.getenv("HOST")
    http_path = os.getenv("HTTP_PATH")  # Load HTTP_PATH from .env
    return access_token, host, http_path

@st.cache_resource
def get_connection(access_token, host, http_path):
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=access_token
    )

def fetch_data(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=headers)

def main():
    st.set_page_config(page_title="Carl Case Categorization Agent", layout="wide")
    
    st.title("Carl Case Categorization")
    st.caption("AI-Powered Case Categorization | Salesforce Case Helper")

    access_token, host, http_path = load_env_vars()  # Get http_path from env

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "👋 Welcome to Carl Case Categorization Agent!\n\nI can help you:\n- Case Categorization\n- 📊 Analyze your case data \n\nTry: \"Which cases has requested by field empty?\""}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "dataframe" in msg and msg["dataframe"] is not None:
                st.dataframe(msg["dataframe"])

    user_input = st.chat_input("Ask about territories, tiering, or deals...")

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        # Example: respond to a specific question
        if "accounts" in user_input.lower() and "prioritize" in user_input.lower():
            conn = get_connection(access_token, host, http_path)
            query = "SELECT Id, Name, Tier FROM access_source.salesforce.account LIMIT 5"
            df = fetch_data(conn, query)
            response = "Based on your data:\n\nPrioritize accounts with the highest number of stalled deals that still show engagement potential. Here are some accounts:"
            st.session_state.messages.append({"role": "assistant", "content": response, "dataframe": df})
        else:
            st.session_state.messages.append({"role": "assistant", "content": "Sorry, I didn't understand your request. Try asking about account prioritization or deals."})

        st.rerun()

if __name__ == "__main__":
    main()