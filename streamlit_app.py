import os
import pandasai as pai
import duckdb, boto3
from urllib.parse import quote
import streamlit as st
from pandasai_litellm import LiteLLM
import pathlib


# ---------- 1. load credentials ----------
def ensure_credentials():
    # AWS credentials
    if not os.getenv("AWS_ACCESS_KEY_ID") and "aws" in st.secrets:
        os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["aws"]["AWS_ACCESS_KEY_ID"]
    if not os.getenv("AWS_SECRET_ACCESS_KEY") and "aws" in st.secrets:
        os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"]

    # OpenAI key
    if not os.getenv("OPENAI_API_KEY") and "openai" in st.secrets:
        os.environ["OPENAI_API_KEY"] = st.secrets["openai"]["OPENAI_API_KEY"]

ensure_credentials()

# ---------- 2. load data ----------
st.set_page_config(page_title="🌩️ NOAA Storm Chatbot", page_icon="🌪️")
@st.cache_data(show_spinner="Loading NOAA 2024 Storm Events Data from Delta Lakehouse..")

def load_from_s3():
    bucket   = "ibhs-lakehouse-poc-1746937696"
    prefix   = "delta/storm_events/"
    region   = "us-east-1"

    # list objects with boto3
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                objs.append({"Key": o["Key"], "LastMod": o["LastModified"]})

    # newest → oldest
    objs.sort(key=lambda x: x["LastMod"], reverse=True)

    # keep the newest copy of every part file
    seen_parts = set()
    keys_kept  = []

    for obj in objs:                               # objs = newest → oldest
        filename = obj["Key"].split("/")[-1]
        part_id  = "-".join(filename.split("-")[:2])   #

        if part_id in seen_parts:      # older duplicate → skip
            continue
        seen_parts.add(part_id)
        keys_kept.append(obj["Key"])   # keep newest "part-xxxxx"

    if not keys_kept:
        raise RuntimeError("No Parquet files found!")

    # feed to DuckDB
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{region}';")

    # build a single comma-separated list of URI strings
    uris = ", ".join(f"'{f's3://{bucket}/{quote(k)}'}'" for k in keys_kept)
    df = con.sql(f"SELECT * FROM read_parquet([{uris}])").df()
    return df

df = pai.DataFrame(load_from_s3())

# ---------- 3. Create LLM wrapper ----------
llm = LiteLLM(model="o4-mini", temperature=1)   # ← o-series needs temp=1

# make this the default LLM for .chat()
pai.config.set({"llm": llm})

# ---------- 4. Streamlit UI ----------
def show_answer(answer: object):
    # 1.  Convert to plain text and strip wrappers / whitespace
    if isinstance(answer, pathlib.Path):
        path = str(answer)
    else:
        path = str(answer).strip().strip("`'\"")   # remove ` ` or quotes

    # 2.  If it still looks like an image file *and* exists, display it
    if path.lower().endswith((".png", ".jpg", ".jpeg", ".svg")) and os.path.exists(path):
        st.image(path, use_column_width=True)
        st.caption(f"[download]({path})")
    else:
        st.success(answer)

# -------------------------------------------------
# 💬  Session-level chat history
# -------------------------------------------------
if "messages" not in st.session_state:
    # each item: {"user": "...", "bot": "..."}   (bot may be str or file path)
    st.session_state.messages = []

# -------------------------------------------------
# UI – previous chat bubbles
# -------------------------------------------------
st.markdown("<h1>🌩️ NOAA 2024 Storm Events Explorer</h1>", unsafe_allow_html=True)
for turn in st.session_state.messages:
    with st.chat_message("user"):
        st.write(turn["user"])
    with st.chat_message("assistant"):
        show_answer(turn["bot"])

# -------------------------------------------------
# UI – input box
# -------------------------------------------------
query = st.chat_input("Ask about the 2024 storm data…")

if query:
    # show user bubble immediately
    with st.chat_message("user"):
        st.write(query)

    with st.spinner("Thinking…"):
        try:
            answer = df.chat(query)
        except Exception as e:
            answer = f"⚠️ {e}"

    # show assistant bubble & capture what was rendered
    with st.chat_message("assistant"):
        rendered = show_answer(answer)

    # save to history
    st.session_state.messages.append({"user": query, "bot": answer})