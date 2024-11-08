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
