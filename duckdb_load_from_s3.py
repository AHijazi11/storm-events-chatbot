import duckdb, boto3
from urllib.parse import quote

def load_from_s3():
    bucket   = "ibhs-lakehouse-poc-1746728142"
    prefix   = "delta/storm_events/"
    region   = "us-east-1"

    # --- 1. list objects with boto3 ---
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                objs.append({"Key": o["Key"], "LastMod": o["LastModified"]})

    # --- 2. newest → oldest ---
    objs.sort(key=lambda x: x["LastMod"], reverse=True)
    print(objs)

    # --- 3. keep the newest copy of every part file ---
    seen_parts = set()
    keys_kept  = []

    for obj in objs:                               # objs = newest → oldest
        filename = obj["Key"].split("/")[-1]
        part_id  = "-".join(filename.split("-")[:2])   # "part-00000"

        if part_id in seen_parts:      # older duplicate → skip
            continue
        seen_parts.add(part_id)
        keys_kept.append(obj["Key"])   # keep newest "part-xxxxx"

    if not keys_kept:
        raise RuntimeError("No Parquet files found!")

    # --- 4. feed to DuckDB ---
    con = duckdb.connect()
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{region}';")

    # build a single comma-separated list of URI strings
    uris = ", ".join(f"'{f's3://{bucket}/{quote(k)}'}'" for k in keys_kept)
    print(uris)
    df = con.sql(f"SELECT * FROM read_parquet([{uris}])").df()
    return df

print(load_from_s3())