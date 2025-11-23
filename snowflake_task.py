# ============================================================
#                IMPORT LIBRARIES
# ============================================================
import os
import pandas as pd
import pandas_gbq
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ============================================================
#                BIGQUERY → DATAFRAME
# ============================================================
BQ_PROJECT = 'reporting-dashboard-423306'

query = """
SELECT *
FROM `reporting-dashboard-423306.bi.TrendFam_Daily_Confirmed_Snapshot`
"""

df = pandas_gbq.read_gbq(query, project_id=BQ_PROJECT)

# Debugging: print columns
print("DataFrame columns after BigQuery fetch:", df.columns)

# ----------------------------------------Date/Time----------------------------------------
df['Month'] = pd.to_datetime(df["Month"], errors='coerce').dt.date
df['Confirmed_Timestamp'] = pd.to_datetime(df['Confirmed_Timestamp'], errors='coerce')

# ============================================================
#                SNOWFLAKE CREDENTIALS (from GitHub Secrets)
# ============================================================
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_WH = os.getenv("SNOW_WH")
SNOW_DB = os.getenv("SNOW_DB")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA")
SNOW_ROLE = os.getenv("SNOW_ROLE")

# ============================================================
#                CONNECT TO SNOWFLAKE
# ============================================================
conn = snowflake.connector.connect(
    user=SNOW_USER,
    password=SNOW_PASSWORD,
    account=SNOW_ACCOUNT,
    warehouse=SNOW_WH,
    database=SNOW_DB,
    schema=SNOW_SCHEMA,
    role=SNOW_ROLE
)

# ============================================================
#     AUTO GENERATE CREATE TABLE BASED ON DATAFRAME SCHEMA
# ============================================================
def generate_table_sql(df, table_name, schema="BI", custom_type_map=None):
    if custom_type_map is None:
        custom_type_map = {}

    type_map = {
        "int64": "NUMBER",
        "float64": "FLOAT",
        "object": "VARCHAR",
        "datetime64[ns]": "TIMESTAMP_NTZ",  # Use proper Snowflake timestamp
        "bool": "BOOLEAN",
    }

    cols = []
    for col, dtype in df.dtypes.items():
        col_name_upper = col.upper()
        snow_type = custom_type_map.get(col_name_upper, type_map.get(str(dtype), "VARCHAR"))
        cols.append(f'"{col_name_upper}" {snow_type}')

    # Use simple triple-quote string with no backslashes in f-string
    return (
        f"CREATE OR REPLACE TABLE {schema}.{table_name} (\n"
        f"    {',\n    '.join(cols)}\n"
        ");"
    )

# ============================================================
#                Change SnowFlake Type
# ============================================================
custom_types = {
    "MONTH": "DATE",
    "SOURCE_TYPE": "VARCHAR",
    "CONFIRMED_TIMESTAMP": "TIMESTAMP_NTZ"
}

table_name = "TrendFam_Daily_Confirmed_Snapshot"
create_sql = generate_table_sql(df, table_name, custom_type_map=custom_types)

cursor = conn.cursor()
cursor.execute(create_sql)
cursor.close()

# Ensure column names are uppercase for Snowflake
df.columns = [col.upper() for col in df.columns]

# ============================================================
#                INSERT DATA INTO SNOWFLAKE
# ============================================================
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df,
    table_name=table_name.upper(),
    auto_create_table=False,
    use_logical_type=True
)

print(f"Upload success: {success}, rows inserted: {nrows}")
