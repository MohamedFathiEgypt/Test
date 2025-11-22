import snowflake.connector
import os

# ---------------------- Snowflake Credentials ----------------------
# سيتم أخذ القيم من GitHub Secrets
user = os.environ.get("SNOW_USER")
password = os.environ.get("SNOW_PASSWORD")
account = os.environ.get("SNOW_ACCOUNT")
warehouse = os.environ.get("SNOW_WH")
database = os.environ.get("SNOW_DB")
schema = os.environ.get("SNOW_SCHEMA")
role = os.environ.get("SNOW_ROLE")

# ---------------------- Connect to Snowflake ----------------------
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)

# ---------------------- Table and Query ----------------------
new_table = 'Github_Test'
source_table = 'PRODUCTION.BOOSTINY_RAW.LINK_RAW_DAILY_VALIDATED'
columns = ['ID', 'Order_ID', 'Country']
limit = 100

query_create = f"""
CREATE OR REPLACE TABLE {new_table} AS
SELECT {', '.join(columns)}
FROM {source_table}
LIMIT {limit};
"""

# ---------------------- Execute Query ----------------------
try:
    cursor = conn.cursor()
    cursor.execute(query_create)
    print(f"Table '{new_table}' created successfully from '{source_table}'!")
finally:
    cursor.close()
    conn.close()
