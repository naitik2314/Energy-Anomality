optin_df=spark.sql("""select * from dev.uncertified.week44_spp_weeklyreminder as a 
                   inner join (select * from common.call_volume_dashboard
                   WHERE to_date(SEGMENT_START, 'yyyy-MM-dd') >= '{start_date}' 
                    AND to_date(SEGMENT_STOP, 'yyyy-MM-dd') <= '{end_date}'
                   ) as b
                   on a.`Business Partner`=b.BUSINESS_PARTNER_ID 
                   and date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd')>=to_date(b.SEGMENT_START,'yyyy-MM-dd') 
                   """)
optin_df.display()
