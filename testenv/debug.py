import os, time, requests, json
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST") 
TOKEN = os.environ.get("DATABRICKS_TOKEN")
SPACE_ID = os.environ.get("GENIE_SPACE_ID")
print(HOST, TOKEN, SPACE_ID)