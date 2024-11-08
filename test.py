group2_df = spark.sql(f"""
WITH filtered_spp AS (
    SELECT DISTINCT BUSINESS_PARTN, START_BB_PERIOD,
        CASE 
            WHEN a.DEACTIVATED IS NOT NULL THEN to_date(a.CHANGED_ON, 'yyyy-MM-dd') 
            ELSE to_date(a.END_BB_PERIOD, 'yyyy-MM-dd') 
        END AS END_DATE
    FROM prod.cds_cods.eabp_v AS a
    WHERE a.PYMT_PLAN_TYPE = 'spp'
      AND a.START_BB_PERIOD <= '{end_date}'
      AND (
          CASE 
              WHEN a.DEACTIVATED IS NOT NULL THEN to_date(a.CHANGED_ON, 'yyyy-MM-dd') 
              ELSE to_date(a.END_BB_PERIOD, 'yyyy-MM-dd') 
          END
      ) >= '{start_date}'
),

excluded_bps AS (
    SELECT DISTINCT `Business Partner` AS BUSINESS_PARTN
    FROM dev.uncertified.week44_spp_weeklyreminder
    WHERE `Opt-in Date` >= '{start_date}' AND `Opt-in Date` <= '{end_date}'
)

SELECT spp.BUSINESS_PARTN, spp.START_BB_PERIOD, spp.END_DATE, dashboard.*
FROM filtered_spp AS spp
LEFT ANTI JOIN excluded_bps AS pilot
ON spp.BUSINESS_PARTN = pilot.BUSINESS_PARTN
INNER JOIN common.call_volume_dashboard AS dashboard
ON spp.BUSINESS_PARTN = dashboard.BUSINESS_PARTNER_ID
  AND spp.START_BB_PERIOD >= dashboard.SEGMENT_START
WHERE dashboard.START_DATE >= '{start_date}'
  AND dashboard.END_DATE < '{end_date}'
""")
