group3_df = spark.sql(f"""
WITH combined_bp AS (
  SELECT DISTINCT BUSINESS_PARTN
  FROM (
    SELECT BUSINESS_PARTN,
      CASE
        WHEN a.DEACTIVATED IS NOT NULL THEN to_date(a.CHANGED_ON, 'yyyy-MM-dd')
        ELSE to_date(a.END_BB_PERIOD, 'yyyy-MM-dd')
      END AS END_DATE
    FROM prod.cds_cods.eabp_v AS a
    WHERE a.PYMT_PLAN_TYPE = 'SPP'
      AND to_date(a.START_BB_PERIOD, 'yyyy-MM-dd') <= '{end_date}'
      AND (
          CASE
            WHEN a.DEACTIVATED IS NOT NULL THEN to_date(a.CHANGED_ON, 'yyyy-MM-dd')
            ELSE to_date(a.END_BB_PERIOD, 'yyyy-MM-dd')
          END
        ) > '{start_date}'
    UNION
      SELECT `Business Partner` AS BUSINESS_PARTN,
        NULL AS END_DATE --Place holder to match column count
      FROM dev.uncertified.week44_spp_weeklyreminder
        WHERE date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
        AND date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}'
  )
)
  SELECT dashboard.BUSINESS_PARTNER_ID, dashboard.UCID, dashboard.prorated_call_new, dashboard.NODE_L3, dashboard.HANDLED_TIME
    FROM common.call_volume_dashboard AS dashboard
    LEFT ANTI JOIN combined_bp
    ON dashboard.BUSINESS_PARTNER_ID = combined_bp.BUSINESS_PARTN
    WHERE to_date(dashboard.SEGMENT_START, 'yyyy-MM-dd') >= '{start_date}' 
      AND to_date(dashboard.SEGMENT_STOP, 'yyyy-MM-dd') <= '{end_date}'
      AND dashboard.NODE_L3 = 'SPP ENROLL'
                      """)
