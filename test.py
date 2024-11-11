import pyspark
import json
from pyspark.sql.types import *
import pandas as pd
import os
import datetime
import time 
import pyspark.sql.functions as f
from typing import List
from datetime import timedelta
from datetime import datetime
import numpy as np
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")

if start_date_str == "" or end_date_str == "":
  # If the widgets are empty, we want the current date time and get the previous week range
  today = datetime.now() # Get the current date time
  last_week_start = today - timedelta(days=today.weekday() + 7) # Returns Last Monday
  last_week_end = last_week_start + timedelta(days=6) # Last Sunday

  start_date = last_week_start.strftime("%Y-%m-%d")
  end_date = last_week_end.strftime("%Y-%m-%d")

else:
  # If the widgets are not empty, then
  start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
  end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

print(f"Using Start Date: {start_date} and End Date:{end_date}, dtypes: Start Date: {type(start_date)} and {type(end_date)}")

# Group 1
optin_df = spark.sql(f"""
SELECT * 
FROM (
  SELECT *
  FROM dev.uncertified.week44_spp_weeklyreminder
          WHERE date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
          AND date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}' 
 ) AS a
INNER JOIN (
    SELECT * 
    FROM common.call_volume_dashboard
    WHERE to_date(SEGMENT_START, 'yyyy-MM-dd') >= to_date('{start_date}', 'yyyy-MM-dd') 
      AND to_date(SEGMENT_STOP, 'yyyy-MM-dd') < to_date('{end_date}', 'yyyy-MM-dd')
      AND NODE_L3 = 'SPP ENROLL'
) AS b
ON a.`Business Partner` = b.BUSINESS_PARTNER_ID 
   AND date_format(to_timestamp(a.`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= to_date(b.SEGMENT_START, 'yyyy-MM-dd')
""")

print(optin_df.select("Business Partner").distinct().count())
optin_df.display()

group2_df = spark.sql(f"""
WITH filtered_spp AS (
    SELECT DISTINCT BUSINESS_PARTN, START_BB_PERIOD,
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
      ) >= '{start_date}'
),

excluded_bps AS (
    SELECT DISTINCT `Business Partner` AS BUSINESS_PARTN
    FROM dev.uncertified.week44_spp_weeklyreminder
        WHERE date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') >= '{start_date}'
        AND date_format(to_timestamp(`Opt-In Date`, 'MM/dd/yy hh:mm a'), 'yyyy-MM-dd') <= '{end_date}'
)

SELECT spp.BUSINESS_PARTN, spp.START_BB_PERIOD, spp.END_DATE, dashboard.*
FROM filtered_spp AS spp
LEFT ANTI JOIN excluded_bps AS pilot
ON spp.BUSINESS_PARTN = pilot.BUSINESS_PARTN
INNER JOIN common.call_volume_dashboard AS dashboard
ON spp.BUSINESS_PARTN = dashboard.BUSINESS_PARTNER_ID
  AND spp.START_BB_PERIOD >= dashboard.SEGMENT_START
WHERE to_date(dashboard.SEGMENT_START, 'yyyy-MM-dd') >= '{start_date}' 
AND to_date(dashboard.SEGMENT_STOP, 'yyyy-MM-dd') <= '{end_date}'
AND dashboard.NODE_L3 = 'SPP ENROLL'
""")

group2_df.display()

# Group 3
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
group3_df = group3_df.dropDuplicates()
group3_df.display()

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def check_and_filter_by_closest_date(df, business_partner_column, date_column, segment_start_column):
    # Step 1: Check uniqueness of the business partner column
    unique_count = df.select(business_partner_column).distinct().count()
    total_count = df.count()
    
    if unique_count == total_count:
        print("Good to go: All values in the specified column are unique.")
    else:
        print("Warning: Some values are duplicated in the specified column.")
    
    # Step 2: Calculate date difference
    df = df.withColumn("date_diff", F.datediff(F.col(date_column), F.col(segment_start_column)))
    
    # Step 3: Define window to find the closest date difference within each group
    window_spec = Window.partitionBy(business_partner_column)
    
    # Step 4: Select the row with the minimum `date_diff` within each group
    df = df.withColumn("selected_row", F.row_number().over(window_spec.orderBy(F.col("date_diff"))))
    
    # Step 5: Filter only the selected rows (keeping the row with the smallest `date_diff`)
    closest_df = df.filter(F.col("selected_row") == 1).drop("date_diff", "selected_row")
    
    # Step 6: Drop duplicates, if any
    closest_df = closest_df.dropDuplicates()
    
    return closest_df

# Calculating AHT
def calculate_average_handle_time(df, handle_time_column):
  total_handle_time = df.agg(f.sum(handle_time_column)).first()[0]
  total_prorated = df.agg(f.sum("prorated_call_new")).first()[0]

  average_handle_time = total_handle_time / total_prorated if total_prorated != 0 else 0

  return total_handle_time, average_handle_time

# Getting the values
group2_df_filtered = check_and_filter_by_closest_date(group2_df, 'BUSINESS_PARTN', 'START_BB_PERIOD', 'SEGMENT_START')
group2_handle_time, group2_avg_handle_time = calculate_average_handle_time(group2_df_filtered, 'HANDLED_TIME')
print(f"Total handle time: {group2_handle_time} and Total number of calls: {group2_df_filtered.count()} and Avg Handle Time: {group2_avg_handle_time}")

# Getting the values
optin_df = optin_df.withColumn("OptIn_Date_Parsed", F.to_timestamp("Opt-In Date", "MM/dd/yy hh:mm a"))
optin_df_filtered = check_and_filter_by_closest_date(optin_df, 'Business Partner', 'OptIn_Date_Parsed', 'SEGMENT_START')
group1_handle_time, group1_avg_handle_time = calculate_average_handle_time(optin_df_filtered, 'HANDLED_TIME')
print(f"Total handle time: {group1_handle_time} and Total number of calls: {optin_df_filtered.count()} and Avg Handle Time: {group1_avg_handle_time}")

# Getting the values
group3_handle_time, group3_avg_handle_time = calculate_average_handle_time(group3_df, 'HANDLED_TIME')
print(f"Total handle time: {group3_handle_time} and Total number of calls: {group3_df.count()} and Avg Handle Time: {group3_avg_handle_time}")
