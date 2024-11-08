optin_df = spark.sql(f"""
SELECT * 
FROM dev.uncertified.week44_spp_weeklyreminder AS a
INNER JOIN (
    SELECT * 
    FROM common.call_volume_dashboard
    WHERE to_date(SEGMENT_START, 'yyyy-MM-dd') >= to_date('{start_date}', 'yyyy-MM-dd') 
      AND to_date(SEGMENT_STOP, 'yyyy-MM-dd') <= to_date('{end_date}', 'yyyy-MM-dd')
) AS b
ON a.`Business Partner` = b.BUSINESS_PARTNER_ID 
   AND date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= to_date(b.SEGMENT_START, 'yyyy-MM-dd')
""")

optin_df.display()
