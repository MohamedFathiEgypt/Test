import os
import snowflake.connector

# ============================================================
#                SNOWFLAKE CREDENTIALS
# ============================================================
# Set these environment variables in GitHub Actions secrets for security
SNOW_USER = os.environ.get("SNOW_USER")
SNOW_PASSWORD = os.environ.get("SNOW_PASSWORD")
SNOW_ACCOUNT = os.environ.get("SNOW_ACCOUNT")
SNOW_WH = os.environ.get("SNOW_WH")
SNOW_DB = os.environ.get("SNOW_DB")
SNOW_SCHEMA = os.environ.get("SNOW_SCHEMA")
SNOW_ROLE = os.environ.get("SNOW_ROLE")

# ============================================================
#                CONNECT TO SNOWFLAKE AND CREATE TABLE
# ============================================================
conn = None
try:
    conn = snowflake.connector.connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        warehouse=SNOW_WH,
        database=SNOW_DB,
        schema=SNOW_SCHEMA,
        role=SNOW_ROLE
    )
    cursor = conn.cursor()

    new_table = "TrendFam_BI_HIS_Current"
    source_table = "PRODUCTION.BI.TRENDFAM_BI_HISTORICAL_ROW"

    select_query = """
    -- Paste your full SELECT query here from Colab
    SELECT * FROM PRODUCTION.BI.TRENDFAM_BI_HISTORICAL_ROW
    UNION ALL
    -- rest of your query ...
    """

    create_ctas_sql = f"""
    CREATE OR REPLACE TABLE {SNOW_SCHEMA}.{new_table} AS
    {select_query};
    """

    print(f"Creating table '{new_table}' in Snowflake...")
    cursor.execute(create_ctas_sql)
    conn.commit()
    print(f"Table '{new_table}' created successfully.")

except Exception as e:
    print(f"Error creating table in Snowflake: {e}")
finally:
    if conn:
        conn.close()
        print("Snowflake connection closed.")
