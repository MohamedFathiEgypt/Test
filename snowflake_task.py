import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ============================================================
#                SNOWFLAKE CREDENTIALS
# ============================================================
SNOW_USER = os.environ.get("SNOW_USER")
SNOW_PASSWORD = os.environ.get("SNOW_PASSWORD")
SNOW_ACCOUNT = os.environ.get("SNOW_ACCOUNT")
SNOW_WH = os.environ.get("SNOW_WH")
SNOW_DB = os.environ.get("SNOW_DB")
SNOW_SCHEMA = os.environ.get("SNOW_SCHEMA")
SNOW_ROLE = os.environ.get("SNOW_ROLE")

# Columns to clean
cols_to_clean = ['Month_Text', 'BU', 'Campaign_Name', 'TL', 'TM', 
                 'Instagram_Name', 'Email', 'Inf_Name', 'CM', 'Country', 
                 'Fixed_RevShare', 'Coupon', 'New_Partner', 'New_Influencer', 
                 'New_Recruit', 'MF_Haitham', 'Campaign_Type', 
                 'Agency_Bounce_Label', 'Is_Payment_Adjusted', 'Added_to_Balance_Variance']

# Table names
new_table = "TrendFam_BI_HIS_Current"
source_table = "PRODUCTION.BI.TRENDFAM_BI_HISTORICAL_ROW"

# ============================================================
#                CONNECT TO SNOWFLAKE
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

    # ============================================================
    #                FETCH RAW DATA (HISTORICAL + CURRENT)
    # ============================================================
    print("Fetching data from Snowflake using your full query...")
    full_query = """
    Select * from PRODUCTION.BI.TRENDFAM_BI_HISTORICAL_ROW union all

    select
        TO_CHAR(DATE, 'MM-MMMM-YYYY') as MONTH_TEXT,
        CAST(month_start AS DATE) AS MONTH_DATE,
        CAST(DATE AS DATE) As DATE,
        source_type AS BU,
        Campaign_Name,
        TL,
        TM,
        INSTAGRAM_NAME,
        email,
        sum(total_revenue) TOTAL_REVENUE ,
        sum(code_bonus) COUPONS,
        Sum(trendfam_earnings) REV,
        sum(cost_link) COST_LINKS,
        sum(cost_coupon)COST_COUPONS,
        INF_NAME,
        COALESCE(CM,CM_TL_NAME) AS CM,
        0 AS AGENCY_BONUS,
        Fixed_cost AS FIXED_COST,
        sum(total_cost) TOTAL_COST,
        country AS COUNTRY,
        SUM(brand_offers_earnings) AS BRAND_OFFER_BONUS,
        FIXED_REVSHARE,
        sum(cash_bonus) BONUS,
        sum(total_rev_share) TOTAL_REVSHARE_COST,
        code COUPON,
        SUM(GMV) GMV,
        sum(new_buyer) NEW_BUYERS,
        COALESCE(DIV0(SUM(GMV),45),0) AS ORDERS_COUPONS,
        COALESCE(DIV0(Sum(trendfam_earnings),45),0) ORDERS_LINKS,
        case when date_trunc('month',REGISTRATION_DATE) = date_trunc('month',date) then 'NEW' else null end as new_partner,
        SUM(brand_offers_earnings) AS BRAND_OFFER_REV,
        SUM(brand_offer_cost) BRAND_OFFER_COST,
        sum(sessions) AS SESSION,
        REGISTRATION_DATE,
        CASE WHEN email IN ('nahlaabosen@gmail.com','lama.alk88@hotmail.com','hneenalhussain@gmail.com','ansaltntawy58@gmail.com','bkshahd@gmail.com','99ama99@gmail.com','rorohomos86@gmail.com','huda.blogger90@gmail.com','soulat4100@gmail.com','yasmaa78@gmail.com','doaa.elseginy@icloud.com') THEN 'Yes' ELSE NULL END AS NEW_INFLUENCER,
        NULL AS NEW_RECRUIT,
        CASE WHEN INSTAGRAM_NAME IN ('alrawm','ZZAA212','r..9115','smasem_albarqi','seham_zh2','nawaraa346','mona_alsharidi','sahabt.suliman','j000ny21','moon25j','drnuni0','sa7ar121','dan_the_riyadh','zakiaalmerri','in.riyadh24','26d1','al__p52','pasma_1','z2au2023','1.pinke','hasa_m','ahd.ii','xlliix5','ivvx_11','asel1012so','HNO141H','afafns','asmahan1404','n.lxz','joo0d_blogger','hayoonh_am','jw.e10','hudahappiness','zoo.xx4x','nojy00','addfhi','rzanaa0') THEN 'MF'
        WHEN INSTAGRAM_NAME IN ('f.alganm','mlakabdllah','osamah201116','mnour144','@linaabyadh','bedayatv','Marwa_rmr','ifarisr','haifa_alballaa','Marmarez81','arqhtn') THEN 'Haithem'
        ELSE  NULL END AS MF_HAITHAM,
        sum(branding_bonus) BRANDING_BONUS,
        sum(branding_bonus_cost) BRANDING_BONUS_COST,
        0 AS FIXED_COST_COUPONS,
        0 AS FIXED_COST_LINKS,
        NULL AS CAMPAIGN_TYPE,
        user_id INF_USER_ID,
        'Normal' As AGENCY_BOUNCE_LABEL,
        'No' As Is_Payment_Adjusted,
        0 As Adjusted_Agency_Bounce,
        0 as Adjusted_Branding_Bounce,
        'No' As Added_to_Balance_Variance
    from (
        -- your full WITH base, inc, allocation logic here
    )
    group by 1,2,3,4,5,6,7,8,9,15,16,17,18,20,22,25,30,34,35,36,37,40,41,42,43,44,45,46,47,48
    order by 2 ASC
    """

    df = pd.read_sql(full_query, conn)

    # ============================================================
    #                CLEAN TEXT COLUMNS BEFORE WRITING
    # ============================================================
    print("Cleaning text columns in Pandas before writing to Snowflake...")
    df[cols_to_clean] = df[cols_to_clean].replace(["", "#N/A", None], pd.NA)
    for col in cols_to_clean:
        df[col] = df[col].astype("string").str.replace(r'[\r\n]+', ' ', regex=True)
        df[col] = df[col].str.strip()  # optional: remove extra spaces

    # ============================================================
    #                CREATE OR REPLACE TABLE IN SNOWFLAKE
    # ============================================================
    print(f"Writing cleaned data to Snowflake table '{new_table}'...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=new_table,
        schema=SNOW_SCHEMA,
        auto_create_table=True,
        overwrite=True
    )
    print(f"Table '{new_table}' created successfully with {nrows} rows.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if conn:
        conn.close()
        print("Snowflake connection closed.")
