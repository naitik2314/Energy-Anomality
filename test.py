# Get unique business partners from `optin_df` and `group2_df`
optin_bps = optin_df.select("Business Partner").distinct()
group2_bps = group2_df.select("BUSINESS_PARTN").distinct()

# Combine the unique business partners from both DataFrames
combined_bps = optin_bps.union(group2_bps).distinct()
combined_bps.createOrReplaceTempView("combined_bps")

group3_df = spark.sql(f"""
SELECT dashboard.BUSINESS_PARTNER_ID, dashboard.SEGMENT_START, dashboard.SEGMENT_STOP, dashboard.NODE_L3, dashboard.HANDLED_TIME
FROM common.call_volume_dashboard AS dashboard
LEFT ANTI JOIN combined_bps
ON dashboard.BUSINESS_PARTNER_ID = combined_bps.`Business Partner`
WHERE to_date(dashboard.SEGMENT_START, 'yyyy-MM-dd') >= '{start_date}' 
  AND to_date(dashboard.SEGMENT_STOP, 'yyyy-MM-dd') <= '{end_date}'
  AND dashboard.NODE_L3 = 'SPP ENROLL'
""")

# Display the final result
group3_df.display()
