# Define the date range
start_date = '2024-08-01'  # Example start date
end_date = '2024-08-31'    # Example end date

# Step 1: Filter `eabp` for active plans within the date range
filtered_eabp = spark.sql(f"""
    SELECT DISTINCT BUSINESS_PARTN,
        CASE 
            WHEN DEACTIVATED IS NOT NULL THEN to_date(CHANGED_ON, 'yyyy-MM-dd') 
            ELSE to_date(END_BB_PERIOD, 'yyyy-MM-dd') 
        END AS END_DATE
    FROM prod.cds_cods.eabp_v
    WHERE PYMT_PLAN_TYPE = 'spp'
      AND START_BB_PERIOD <= '{end_date}' 
      AND (
          CASE 
              WHEN DEACTIVATED IS NOT NULL THEN to_date(CHANGED_ON, 'yyyy-MM-dd')
              ELSE to_date(END_BB_PERIOD, 'yyyy-MM-dd')
          END
      ) >= '{start_date}'
""")

# Step 2: Filter `week44_spp_weeklyreminder` for records active within the date range
filtered_week44 = spark.sql(f"""
    SELECT DISTINCT `Business Partner` AS BUSINESS_PARTNER
    FROM dev.uncertified.week44_spp_weeklyreminder
    WHERE to_date(`Opt-In Date`, 'MM/dd/yy hh:mm a') >= '{start_date}'
      AND to_date(`Opt-In Date`, 'MM/dd/yy hh:mm a') <= '{end_date}'
""")

# Step 3: Combine unique Business Partners from both filtered datasets
combined_bp = filtered_eabp.select("BUSINESS_PARTN").union(filtered_week44.select("BUSINESS_PARTNER")).distinct()

# Step 4: Exclude combined Business Partners from `call_volume_dashboard`
group3_df = spark.sql("""
    SELECT * 
    FROM common.call_volume_dashboard
""").join(combined_bp, common.call_volume_dashboard.BUSINESS_PARTNER_ID == combined_bp.BUSINESS_PARTN, "left_anti")

# Step 5: Drop duplicate rows from `call_volume_dashboard` after filtering
group3_df = group3_df.dropDuplicates()

group3_df.show()
