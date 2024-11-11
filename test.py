optin_df = spark.sql(f"""
SELECT a.`Business Partner`, a.`Opt-In Date`, b.SEGMENT_START, b.SEGMENT_STOP, b.NODE_L3, b.HANDLED_TIME
FROM dev.uncertified.week44_spp_weeklyreminder AS a
INNER JOIN common.call_volume_dashboard AS b
ON a.`Business Partner` = b.BUSINESS_PARTNER_ID 
WHERE date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
  AND date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}' 
  AND to_date(b.SEGMENT_START, 'yyyy-MM-dd') >= to_date('{start_date}', 'yyyy-MM-dd') 
  AND to_date(b.SEGMENT_STOP, 'yyyy-MM-dd') < to_date('{end_date}', 'yyyy-MM-dd')
  AND b.NODE_L3 = 'SPP ENROLL'
  AND date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= to_date(b.SEGMENT_START, 'yyyy-MM-dd')
""")
optin_df.display()
