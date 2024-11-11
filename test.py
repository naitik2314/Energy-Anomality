# Step 1: Define `excluded_bps` and register as a temporary view
excluded_bps = spark.sql(f"""
SELECT DISTINCT `Business Partner` AS BUSINESS_PARTN
FROM dev.uncertified.week44_spp_weeklyreminder
WHERE date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
  AND date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}'
""")
excluded_bps.createOrReplaceTempView("excluded_bps")

# Step 2: Define `filtered_spp` and perform LEFT ANTI JOIN with `excluded_bps`
group2_df = spark.sql(f"""
WITH filtered_spp AS (
    SELECT DISTINCT BUSINESS_PARTN, START_BB_PERIOD,
        CASE 
            WHEN DEACTIVATED IS NOT NULL THEN to_date(CHANGED_ON, 'yyyy-MM-dd') 
            ELSE to_date(END_BB_PERIOD, 'yyyy-MM-dd') 
        END AS END_DATE
    FROM prod.cds_cods.eabp_v
    WHERE PYMT_PLAN_TYPE = 'SPP'
      AND to_date(START_BB_PERIOD, 'yyyy-MM-dd') <= '{end_date}'
      AND (CASE WHEN DEACTIVATED IS NOT NULL THEN to_date(CHANGED_ON, 'yyyy-MM-dd') ELSE to_date(END_BB_PERIOD, 'yyyy-MM-dd') END) >= '{start_date}'
)

SELECT spp.BUSINESS_PARTN, spp.START_BB_PERIOD, spp.END_DATE, dashboard.*
FROM filtered_spp AS spp
LEFT ANTI JOIN excluded_bps AS pilot
ON spp.BUSINESS_PARTN = pilot.BUSINESS_PARTN
INNER JOIN (
    SELECT * 
    FROM common.call_volume_dashboard
    WHERE to_date(SEGMENT_START, 'yyyy-MM-dd') >= '{start_date}' 
      AND to_date(SEGMENT_STOP, 'yyyy-MM-dd') <= '{end_date}'
      AND NODE_L3 = 'SPP ENROLL'
) AS dashboard
ON spp.BUSINESS_PARTN = dashboard.BUSINESS_PARTNER_ID
  AND spp.START_BB_PERIOD >= dashboard.SEGMENT_START
""")

# Display the final DataFrame
group2_df.display()
