import pandas as pd
import duckdb, boto3
from urllib.parse import quote
import streamlit as st
from langchain.chat_models import ChatOpenAI
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType

# ---------- 2. load your data ----------
# df = pd.read_csv("cleaned_storm_events.csv")
st.set_page_config(page_title="🌩️ NOAA Storm Chatbot", page_icon="🌪️")
@st.cache_data(show_spinner="Loading from S3…")

def load_from_s3():
    bucket   = "ibhs-lakehouse-poc-1746728142"
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
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{region}';")

    # build a single comma-separated list of URI strings
    uris = ", ".join(f"'{f's3://{bucket}/{quote(k)}'}'" for k in keys_kept)
    print(uris)
    df = con.sql(f"SELECT * FROM read_parquet([{uris}])").df()
    return df

df = load_from_s3()
print(df.info())

system_prompt = (
    "You are an expert data assistant. "
    "When you need to run Python, call the tool exactly as "
    "`python_repl_ast`, **without square brackets**."
)

# ---------- 3. initialise the LLM ----------
llm = ChatOpenAI(
    model="gpt-3.5-turbo-0125",  
    temperature=0
)

# llm = llm.bind(system=system_prompt)   # bind the prompt once

# ---------- 4. build the agent ----------
agent = create_pandas_dataframe_agent(
    llm, df,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,   # avoids function/tool roles
    prefix_messages=[{"role": "system", "content": system_prompt}],
    verbose=True,
    handle_parsing_errors=True,
    allow_dangerous_code=True
)

# ---------- 5. Streamlit UI ----------

st.markdown("<h1>🌩️ NOAA Storm Events Explorer</h1>", unsafe_allow_html=True)
query = st.text_input("Ask about the 2024 storm data:")

if query:
    with st.spinner("Thinking..."):
        try:
            answer = agent.run(query)
            st.success(answer)
        except Exception as e:
            st.error(f"⚠️ {e}")
