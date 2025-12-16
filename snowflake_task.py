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
            CASE WHEN email IN      ('nahlaabosen@gmail.com','lama.alk88@hotmail.com','hneenalhussain@gmail.com','ansaltntawy58@gmail.com','bkshahd@gmail.com','99ama99@gmail.com','rorohomos86@gmail.com','huda.blogger90@gmail.com','soulat4100@gmail.com','yasmaa78@gmail.com','doaa.elseginy@icloud.com') THEN 'Yes' ELSE NULL END AS NEW_INFLUENCER,

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
            -- sum(number_of_transactions) number_of_transactions,




from (


with base as (


               select
                        DATE,
                        month_start,
                        source_type,
                        INSTAGRAM_NAME,
                        code,
                        user_id,
                        email,
                        COUNTRY,
                        coalesce(sum(total_earnings),0) total_revenue,
                        coalesce(sum(trendfam_earnings),0) trendfam_earnings ,
                        coalesce(sum(code_bonus),0) code_bonus,
                        coalesce(sum(cash_bonus),0) cash_bonus,
                        coalesce(sum(branding_bonus),0) branding_bonus,
                        coalesce(sum(brand_offers_bonus),0) brand_offers_bonus,
                        coalesce(sum(brand_offers_earnings),0) brand_offers_earnings,
                        coalesce(sum(sessions),0) sessions,
                        sum(new_buyer)new_buyer,
                        sum(GMV)GMV,
                        sum(number_of_transactions) number_of_transactions
                         -- coalesce(total_revenue,0) total_revenue
                from (
                        select  DATE,
                        date_trunc('month',date ) month_start ,
                        source_type,
                        INSTAGRAM_NAME,
                        created_at,
                        code,
                        user_id,
                        email,
                        COUNTRY,
                        coalesce(sum(total_earnings),0) total_earnings,
                        coalesce(sum(trendfam_earnings),0) as trendfam_earnings ,
                        coalesce(sum(code_bonus),0) as code_bonus ,
                        coalesce(sum(cash_bonus),0) as cash_bonus,
                        coalesce(sum(branding_bonus),0)  as branding_bonus,
                        coalesce(sum(brand_offers_bonus),0) as brand_offers_bonus,
                        coalesce(sum(brand_offers_earnings),0) as brand_offers_earnings ,
                        coalesce(sum(sessions),0) sessions,
                        sum(new_buyer) new_buyer,
                        (coalesce(sum(trendfam_earnings),0)
                        +coalesce(sum(code_bonus),0)
                        +coalesce(sum(cash_bonus),0)
                        +coalesce(sum(brand_offers_earnings),0)
                        +coalesce(sum(brand_offers_bonus),0)
                        +coalesce(sum(branding_bonus),0) )
                        as total_revenue,
                        sum(net_revenue) as GMV,
                        sum(transactions) number_of_transactions,
                        rank() over (partition by  date_trunc('month',date ),DATE  ,source_type order by created_at DESC) rnk

                 from production.perf_bi_raw.trendfam_conversions
                 where date_trunc('month',DATE ) = date_trunc('month',current_date())
                 -- and INSTAGRAM_NAME = 'Kodjy.Store'

                group by 1,2,3,4,5,6,7,8
                ,9
                order by created_at DESC,INSTAGRAM_NAME ASC
                )
                where rnk = 1
                group by
                        DATE,
                        month_start,
                        source_type,
                        INSTAGRAM_NAME,
                        code,
                        user_id,
                        email,
                        COUNTRY
                -- and INSTAGRAM_NAME = 'nalleosman'
                -- and INSTAGRAM_NAME = 'otlob_review'

                )
, inc as (
            select  distinct
            bu,CM_TL_NAME,INSTA_NAME,
             cast(SUBSTR(INCENTIVE_PERCENT, 1, LENGTH(INCENTIVE_PERCENT) - 1) AS FLOAT) / 100 AS   INCENTIVE_PERCENT ,models,month
          from   PRODUCTION.PERF_BI_RAW.TRENDFAM_INCENTIVES where month = date_trunc('month',current_date())
          order by 3
 -- where insta_name= 'nalleosman'
)
, allocation AS (

            select distinct  *
             from PRODUCTION.BI.TRENDFAM_HITORICAL_ALLOCATION
             where month = date_trunc('month',current_date())
             order by 7 desc



)

    select
        distinct
                base.month_start,
                base.DATE,
                base.source_type,
                'TrendFam' as Campaign_Name,
                null as  type,
                base.INSTAGRAM_NAME,
                allocation.INF_NAME,
                case when
                base.country = 'Kuwait' then 'KWT'
                when base.country = 'United Arab Emirates'then 'UAE'
                when base.country = 'Saudi Arabia' then 'KSA'
                when base.country = 'Qatar'then 'QT'
                when base.country = 'Bahrain' then 'BHR'
                when base.country = 'Oman'then 'OM'
                else null end as country,
                code,
                user_id,
                email,
                base.total_revenue,
                base.trendfam_earnings ,
                base.code_bonus,
                base.cash_bonus,
                base.branding_bonus,
                base.brand_offers_bonus,
                base.brand_offers_earnings,
                null as Fixed_cost,
                coalesce( round(
                coalesce( null,0) +
                case when base.source_type = 'iconnect' and  allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' then base.trendfam_earnings* coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  allocation.FIXED_REVSHARE = 'RevShare' and inc_l.models in ('Link','links')  then base.trendfam_earnings*                                                                                 coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                else base.trendfam_earnings *0.85 end
                +
                case when base.source_type = 'iconnect' and  allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' then base.branding_bonus* coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  allocation.FIXED_REVSHARE = 'RevShare' and inc_l.models in ('Link','links')  then base.branding_bonus*                                                                                 coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                else base.branding_bonus *0.85 end
                +
                 case when
                base.source_type = 'iconnect' and allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' and inc_c.models = 'coupons'  then  base.code_bonus * inc_c.INCENTIVE_PERCENT
                when base.source_type = 'iconnect'and allocation.FIXED_REVSHARE = 'RevShare' then base.code_bonus * 0.5
                else
                coalesce(base.code_bonus * 0.5,0.5) end
                +
                base.cash_bonus
                +
                case when allocation.FIXED_REVSHARE = 'Fixed' then 0 when  allocation.FIXED_REVSHARE = 'RevShare'
                then base.branding_bonus* coalesce(inc_l.INCENTIVE_PERCENT,0.85) else base.branding_bonus *0.85 end
                +
                case when allocation.FIXED_REVSHARE = 'RevShare' and base.source_type = 'iconnect' then base.brand_offers_earnings *                                                                            coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  base.source_type = 'revgate'  then base.brand_offers_earnings * coalesce(inc_l.INCENTIVE_PERCENT,0.5)
                end
                +
                base.brand_offers_bonus,2
                ),0) as total_cost,
                base.sessions,
                -- base.COUNTRY,
                coalesce(inc_l.INCENTIVE_PERCENT,0.85) as link_incentive,
                coalesce(inc_c.INCENTIVE_PERCENT,0.5) as Coupon_incentive ,
                allocation.FIXED_REVSHARE,
                coalesce(inc_l.CM_TL_NAME,inc_c.CM_TL_NAME) CM_TL_NAME,
                allocation.TL,
                allocation.TM,allocation.CM,
                allocation.REGISTRATION_DATE,


                case when base.source_type = 'iconnect' and  allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' then base.trendfam_earnings* coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  allocation.FIXED_REVSHARE = 'RevShare' and inc_l.models in ('Link','links')  then base.trendfam_earnings*                                                                                 coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                else base.trendfam_earnings *0.85 end as cost_link,

                case when
                base.source_type = 'iconnect' and allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' and inc_c.models = 'coupons'  then  base.code_bonus * inc_c.INCENTIVE_PERCENT
                when base.source_type = 'iconnect'and allocation.FIXED_REVSHARE = 'RevShare' then base.code_bonus * 0.5
                else
                coalesce(base.code_bonus * 0.5,0.5) end
                as cost_coupon,

                case when
                allocation.FIXED_REVSHARE = 'RevShare' and
                base.source_type = 'iconnect' then base.brand_offers_earnings *coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when
                -- allocation.FIXED_REVSHARE = 'RevShare' and
                base.source_type = 'revgate'  then base.brand_offers_earnings * coalesce(inc_l.INCENTIVE_PERCENT,0.5)
                end as brand_offer_cost,


                case when base.source_type = 'iconnect' and  allocation.FIXED_REVSHARE = 'Fixed' then 0
                when base.source_type = 'revgate' then base.branding_bonus* coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  allocation.FIXED_REVSHARE = 'RevShare' and inc_l.models in ('Link','links')  then base.branding_bonus*                                                                                 coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                else base.branding_bonus *0.85 end as branding_bonus_cost,


                (case when allocation.FIXED_REVSHARE = 'Fixed' then 0 when  allocation.FIXED_REVSHARE = 'RevShare'
                then base.trendfam_earnings* coalesce(inc_l.INCENTIVE_PERCENT,0.85) else base.trendfam_earnings *0.85 end
                +
                case when allocation.FIXED_REVSHARE = 'Fixed' then 0
                when allocation.FIXED_REVSHARE = 'RevShare' and base.source_type = 'iconnect' then base.code_bonus * 0.5
                when base.source_type = 'revgate' then base.code_bonus * inc_l.INCENTIVE_PERCENT end
                +
                base.cash_bonus
                +
                case when allocation.FIXED_REVSHARE = 'RevShare' and base.source_type = 'iconnect' then base.brand_offers_earnings *                                                                            coalesce(inc_l.INCENTIVE_PERCENT,0.85)
                when  base.source_type = 'revgate'  then base.brand_offers_earnings * coalesce(inc_l.INCENTIVE_PERCENT,0.5)
                end
                +
                base.brand_offers_bonus

                ) as total_rev_share,
                base.new_buyer,
                base.GMV,
                base.number_of_transactions
                -- ,case when date_trunc('month',allocation.REGISTRATION_DATE) = date_trunc('month',date) then 'NEW' else null end as new_partner





    from base
    left join inc as inc_l
    on base.month_start = inc_l.month and base.INSTAGRAM_NAME = inc_l.INSTA_NAME
    and lower(base.source_type) = lower(inc_l.BU)
    and inc_l.models in ('Link','links')
    left join allocation on base.month_start = allocation.month and base.INSTAGRAM_NAME = allocation.instagram_name
    and lower(base.source_type) = lower(allocation.bu)
    left join  inc as inc_c
    on base.month_start = inc_c.month and base.INSTAGRAM_NAME = inc_c.INSTA_NAME
    and lower(base.source_type) = lower(inc_c.BU) and inc_c.models = 'coupons'

    where
     -- base.INSTAGRAM_NAME = 'joudalahmadii' and
    MONTH_START = date_trunc('month',current_date())

    )
    -- WHERE source_type <> 'iconnect'
    group by 1,2,3,4,5,6,7,8,9,15,16,17,18,20,22,25,30,34,35,36,37,40,41,42,43,44,45,46,47,48
    order by 2 ASC


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
