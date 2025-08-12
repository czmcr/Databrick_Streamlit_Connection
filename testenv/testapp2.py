import os, time, requests, json
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST") 
TOKEN = os.environ.get("DATABRICKS_TOKEN")
SPACE_ID = os.environ.get("GENIE_SPACE_ID")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# if not (HOST and TOKEN and SPACE_ID):
#     st.error("Set DATABRICKS_HOST, DATABRICKS_TOKEN, and GENIE_SPACE_ID env vars.")
#     st.stop()

# ---------------- Genie API helpers ----------------
def start_conversation(user_text):
    url = f"{HOST}/api/2.0/genie/spaces/{SPACE_ID}/start-conversation"
    payload = {"content": user_text}
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()

def send_message(conversation_id, user_text):
    url = f"{HOST}/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages"
    payload = {"messages": [{"role": "user", "text": user_text}]}
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()

def get_message(space_id, conversation_id, message_id):
    url = f"{HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def poll_message(space_id, conversation_id, message_id, timeout_s=600):
    start = time.time()
    wait = 5
    while time.time() - start < timeout_s:
        msg = get_message(space_id, conversation_id, message_id)
        status = msg.get("status")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            return msg
        time.sleep(wait)
        if time.time() - start > 120:
            wait = min(wait * 2, 60)
    raise TimeoutError("No final message status within timeout")

def fetch_query_result(space_id, conversation_id, message_id, attachment_id):
    url = f"{HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

# ---------------- Streamlit UI ----------------
st.title("Genie — Streamlit Chat (REST API)")

if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Display previous messages
for role, content in st.session_state.chat_history:
    if role == "user":
        st.markdown(f"**You:** {content}")
    else:
        st.markdown(f"**Genie:** {content}")

# Input box at bottom
q = st.text_input("Type your question or follow-up:")

if st.button("Send") and q.strip():
    with st.spinner("Sending to Genie..."):
        # First message in a new conversation
        if st.session_state.conversation_id is None:
            r = start_conversation(q)
            conv = r.get("conversation", {})
            msg = r.get("message", {})
            conv_id = conv.get("id")
            msg_id = msg.get("id")
            st.session_state.conversation_id = conv_id
        # Follow-up in existing conversation
        else:
            conv_id = st.session_state.conversation_id
            r = send_message(conv_id, q)
            msg = r.get("message", {})
            msg_id = msg.get("id")

        # Store user message in chat history
        st.session_state.chat_history.append(("user", q))

        # Wait for Genie to respond
        final_msg = poll_message(SPACE_ID, conv_id, msg_id)
        genie_text = final_msg.get("text") or final_msg.get("content") or ""
        st.session_state.chat_history.append(("genie", genie_text))

        # Display Genie’s reply
        st.subheader("Genie text")
        st.write(genie_text)

        # Handle attachments
        attachments = final_msg.get("attachments") or []
        if attachments:
            for a in attachments:
                if a.get("query"):
                    st.subheader("Generated SQL")
                    st.code(a["query"], language="sql")

                attachment_id = a.get("attachment_id")
                if attachment_id:
                    res = fetch_query_result(SPACE_ID, conv_id, msg_id, attachment_id)
                    try:
                        columns = [col["name"] for col in res["statement_response"]["manifest"]["schema"]["columns"]]
                        data = res["statement_response"]["result"]["data_array"]
                        df = pd.DataFrame(data, columns=columns)
                        st.subheader("Query Results")
                        st.dataframe(df, use_container_width=True)
                    except KeyError:
                        st.warning("Could not parse results into a table — showing raw JSON.")
                        st.json(res)
        else:
            st.info("No attachments (no SQL / results produced).")
