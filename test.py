# Group 1
optin_df = spark.sql(f"""
SELECT * 
(FROM dev.uncertified.week44_spp_weeklyreminder
          WHERE date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
          AND date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}' 
 ) AS a
INNER JOIN (
    SELECT * 
    FROM common.call_volume_dashboard
    WHERE to_date(SEGMENT_START, 'yyyy-MM-dd') >= to_date('{start_date}', 'yyyy-MM-dd') 
      AND to_date(SEGMENT_STOP, 'yyyy-MM-dd') <= to_date('{end_date}', 'yyyy-MM-dd')
      AND NODE_L3 = 'SPP ENROLL'
) AS b
ON a.`Business Partner` = b.BUSINESS_PARTNER_ID 
   AND date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= to_date(b.SEGMENT_START, 'yyyy-MM-dd')
""")

print(optin_df.select("Business Partner").distinct().count())
optin_df.display()
